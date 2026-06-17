/*******************************************************************************
 * Copyright (c) 2025.
 * IWIN-FINS Lab, Shanghai Jiao Tong University, Shanghai, China.
 * All rights reserved.
 ******************************************************************************/

// action/action_manager.cpp

#include <fins/action/action_manager.hpp>
#include <fins/utils/logger.hpp>
#include <pthread.h>

namespace fins {

  ActionManager &ActionManager::get_instance() {
    static ActionManager instance;
    return instance;
  }

  ActionManager::ActionManager() : stop_(false) { worker_thread_ = std::thread(&ActionManager::worker_loop, this); }

  ActionManager::~ActionManager() {
    stop_ = true;
    cv_.notify_all();
    if (worker_thread_.joinable()) {
      worker_thread_.join();
    }
  }

  void ActionManager::register_commander(const std::string &action_name, std::type_index goal_type_id,
                                         std::type_index feedback_type_id, ResultCallback result_cb,
                                         FeedbackCallback feedback_cb) {
    std::lock_guard<std::mutex> lock(map_mutex_);
    if (commanders_.find(action_name) != commanders_.end()) {
      FINS_LOG_ERROR("[ActionManager] Duplicate commander name detected: '{}'. "
                     "The second registration will overwrite the first.", action_name);
    }
    commanders_[action_name] = {result_cb, feedback_cb, goal_type_id, feedback_type_id};
  }

  void ActionManager::register_actor(const std::string &action_name, std::type_index goal_type_id,
                                     std::type_index feedback_type_id, GoalCallback goal_cb) {
    std::lock_guard<std::mutex> lock(map_mutex_);
    if (actors_.find(action_name) != actors_.end()) {
      FINS_LOG_ERROR("[ActionManager] Duplicate actor name detected: '{}'. "
                     "The second registration will overwrite the first.", action_name);
    }
    actors_[action_name] = {goal_cb, goal_type_id, feedback_type_id, false};
  }

  std::shared_ptr<ActionSessionBase> ActionManager::create_action_session(const std::string &action_name,
                                                                           std::vector<std::any> goal_args,
                                                                           std::type_index goal_type_id,
                                                                           std::type_index feedback_type_id) {
    std::shared_ptr<ActionSessionBase> session;

    {
      std::lock_guard<std::mutex> lock(map_mutex_);

      auto actor_it = actors_.find(action_name);
      if (actor_it != actors_.end() && actor_it->second.is_executing) {
        throw std::runtime_error("[ActionManager] Actor '" + action_name + "' is already executing a task");
      }

      auto cmd_it = commanders_.find(action_name);
      if (cmd_it == commanders_.end()) {
        throw std::runtime_error("[ActionManager] Commander '" + action_name + "' not registered");
      }

      if (cmd_it->second.goal_type_id != goal_type_id || cmd_it->second.feedback_type_id != feedback_type_id) {
        throw std::runtime_error("[ActionManager] Type mismatch for action '" + action_name + "'");
      }

      class TypeErasedActionSession : public ActionSessionBase {
      public:
        TypeErasedActionSession(const std::string &name, FeedbackCallback fb_cb, ResultCallback res_cb,
                                std::function<void()> on_complete_cb)
            : action_name_(name), feedback_callback_(fb_cb), result_callback_(res_cb), 
              on_complete_callback_(on_complete_cb), state_(ActionState::Accepted) {}

        ActionState get_state() const override {
          std::lock_guard<std::mutex> lock(mutex_);
          return state_;
        }

        void cancel() override {
          std::lock_guard<std::mutex> lock(mutex_);
          if (state_ == ActionState::Accepted || state_ == ActionState::Executing) {
            state_ = ActionState::Canceling;
          }
        }

        std::string get_action_name() const override { return action_name_; }

        bool is_canceling() const override {
          std::lock_guard<std::mutex> lock(mutex_);
          return state_ == ActionState::Canceling;
        }

        void succeed() override {
          ResultCallback callback_copy;
          std::function<void()> complete_callback_copy;
          {
            std::lock_guard<std::mutex> lock(mutex_);
            state_ = ActionState::Succeeded;
            callback_copy = result_callback_;
            complete_callback_copy = on_complete_callback_;
          }
          if (callback_copy) {
            callback_copy(ActionState::Succeeded);
          }
          if (complete_callback_copy) {
            complete_callback_copy();
          }
        }

        void abort() override {
          ResultCallback callback_copy;
          std::function<void()> complete_callback_copy;
          {
            std::lock_guard<std::mutex> lock(mutex_);
            state_ = ActionState::Aborted;
            callback_copy = result_callback_;
            complete_callback_copy = on_complete_callback_;
          }
          if (callback_copy) {
            callback_copy(ActionState::Aborted);
          }
          if (complete_callback_copy) {
            complete_callback_copy();
          }
        }

        void set_canceled() override {
          ResultCallback callback_copy;
          std::function<void()> complete_callback_copy;
          {
            std::lock_guard<std::mutex> lock(mutex_);
            if (state_ == ActionState::Canceling) {
              state_ = ActionState::Canceled;
              callback_copy = result_callback_;
              complete_callback_copy = on_complete_callback_;
            }
          }
          if (callback_copy) {
            callback_copy(ActionState::Canceled);
          }
          if (complete_callback_copy) {
            complete_callback_copy();
          }
        }

      protected:
        void feedback_impl(std::vector<std::any> args) override {
          FeedbackCallback callback_copy;
          {
            std::lock_guard<std::mutex> lock(mutex_);
            callback_copy = feedback_callback_;
          }
          if (callback_copy) {
            callback_copy(args);
          }
        }

      private:
        std::string action_name_;
        FeedbackCallback feedback_callback_;
        ResultCallback result_callback_;
        std::function<void()> on_complete_callback_;
        mutable std::mutex mutex_;
        ActionState state_;
      };

      auto on_complete = [this, action_name]() {
        std::lock_guard<std::mutex> lock(map_mutex_);
        auto actor_it = actors_.find(action_name);
        if (actor_it != actors_.end()) {
          actor_it->second.is_executing = false;
          FINS_LOG_INFO("[ActionManager] Actor '{}' task completed, ready for next task", action_name);
        }
        active_sessions_.erase(action_name);
      };

      session = std::make_shared<TypeErasedActionSession>(
          action_name, cmd_it->second.feedback_callback, cmd_it->second.result_callback, on_complete);

      active_sessions_[action_name] = session;
    }

    {
      std::lock_guard<std::mutex> lock(queue_mutex_);
      goal_tasks_.push_back({action_name, std::move(goal_args), goal_type_id, feedback_type_id});
      cv_.notify_one();
    }

    return session;
  }

  void ActionManager::cancel_action(const std::string &action_name) {
    std::lock_guard<std::mutex> lock(map_mutex_);

    auto session_it = active_sessions_.find(action_name);
    if (session_it != active_sessions_.end()) {
      session_it->second->cancel();
    }
  }

  ActionState ActionManager::get_action_state(const std::string &action_name) {
    std::lock_guard<std::mutex> lock(map_mutex_);

    auto session_it = active_sessions_.find(action_name);
    if (session_it != active_sessions_.end()) {
      return session_it->second->get_state();
    }

    return ActionState::Aborted;
  }

  void ActionManager::worker_loop() {
    pthread_setname_np(pthread_self(), "fins_action");
    while (!stop_) {
      GoalTask task;

      {
        std::unique_lock<std::mutex> lock(queue_mutex_);
        cv_.wait(lock, [this] { return !goal_tasks_.empty() || stop_; });

        if (stop_)
          break;

        if (goal_tasks_.empty())
          continue;

        task = std::move(goal_tasks_.front());
        goal_tasks_.pop_front();
      }

      try {
        std::shared_ptr<ActionSessionBase> session;
        
        {
          std::lock_guard<std::mutex> lock(map_mutex_);

          auto actor_it = actors_.find(task.action_name);
          if (actor_it == actors_.end()) {
            FINS_LOG_ERROR("[ActionManager] Actor '{}' not found", task.action_name);
            continue;
          }

          auto session_it = active_sessions_.find(task.action_name);
          if (session_it == active_sessions_.end()) {
            FINS_LOG_ERROR("[ActionManager] Session for '{}' not found", task.action_name);
            continue;
          }
          session = session_it->second;

          actor_it->second.is_executing = true;

          if (actor_it->second.goal_callback) {
            actor_it->second.goal_callback(session, task.goal_args);
          }
        }

      } catch (const std::exception &e) {
        FINS_LOG_ERROR("[ActionManager] Error processing goal for '{}': {}", task.action_name, e.what());
      }
    }
  }

} // namespace fins
