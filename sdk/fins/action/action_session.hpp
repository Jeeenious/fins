/*******************************************************************************
 * Copyright (c) 2025.
 * IWIN-FINS Lab, Shanghai Jiao Tong University, Shanghai, China.
 * All rights reserved.
 ******************************************************************************/

// action/action_session.hpp

#pragma once

#include <fins/action/action_tags.hpp>
#include <any>
#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace fins {

  class ActionSessionBase {
  public:
    virtual ~ActionSessionBase() = default;
    virtual ActionState get_state() const = 0;
    virtual void cancel() = 0;
    virtual std::string get_action_name() const = 0;

    template<typename... FeedbackArgs>
    void feedback(FeedbackArgs &&...args) {
      feedback_impl({std::any(std::forward<FeedbackArgs>(args))...});
    }

    virtual void succeed() = 0;
    virtual void abort() = 0;
    virtual void set_canceled() = 0;
    virtual bool is_canceling() const = 0;

  protected:
    virtual void feedback_impl(std::vector<std::any> args) = 0;
  };

  template<typename GoalTuple, typename FeedbackTuple>
  class ActionSession : public ActionSessionBase, public std::enable_shared_from_this<ActionSession<GoalTuple, FeedbackTuple>> {
  public:
    using FeedbackCallback = std::function<void(const FeedbackTuple &)>;
    using ResultCallback = std::function<void(ActionState)>;

    ActionSession(const std::string &action_name, const GoalTuple &goal, FeedbackCallback feedback_cb,
                  ResultCallback result_cb) :
        action_name_(action_name),
        goal_(goal), feedback_callback_(feedback_cb), result_callback_(result_cb), state_(ActionState::Accepted) {}

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

    void accept() {
      std::lock_guard<std::mutex> lock(mutex_);
      if (state_ == ActionState::Accepted) {
        state_ = ActionState::Accepted;
      }
    }

    void execute() {
      std::lock_guard<std::mutex> lock(mutex_);
      if (state_ == ActionState::Accepted) {
        state_ = ActionState::Executing;
      }
    }

    void succeed() override {
      std::lock_guard<std::mutex> lock(mutex_);
      if (state_ == ActionState::Executing) {
        state_ = ActionState::Succeeded;
        if (result_callback_) {
          result_callback_(ActionState::Succeeded);
        }
      }
    }

    void abort() override {
      std::lock_guard<std::mutex> lock(mutex_);
      state_ = ActionState::Aborted;
      if (result_callback_) {
        result_callback_(ActionState::Aborted);
      }
    }

    void set_canceled() override {
      std::lock_guard<std::mutex> lock(mutex_);
      if (state_ == ActionState::Canceling) {
        state_ = ActionState::Canceled;
        if (result_callback_) {
          result_callback_(ActionState::Canceled);
        }
      }
    }

    template<typename... FeedbackArgs>
    void feedback(FeedbackArgs &&...args) {
      std::lock_guard<std::mutex> lock(mutex_);
      if (feedback_callback_) {
        FeedbackTuple fb_data = std::make_tuple(std::forward<FeedbackArgs>(args)...);
        feedback_callback_(fb_data);
      }
    }

    const GoalTuple &get_goal() const { return goal_; }

  protected:
    void feedback_impl(std::vector<std::any> args) override {
      std::lock_guard<std::mutex> lock(mutex_);
      if (feedback_callback_) {
        FeedbackTuple fb_data = unpack_feedback(args, std::make_index_sequence<std::tuple_size_v<FeedbackTuple>>{});
        feedback_callback_(fb_data);
      }
    }

  private:
    template<size_t... Is>
    FeedbackTuple unpack_feedback(const std::vector<std::any> &args, std::index_sequence<Is...>) {
      return std::make_tuple(std::any_cast<std::tuple_element_t<Is, FeedbackTuple>>(args[Is])...);
    }

    std::string action_name_;
    GoalTuple goal_;
    FeedbackCallback feedback_callback_;
    ResultCallback result_callback_;
    mutable std::mutex mutex_;
    ActionState state_;
  };

  template<typename GoalTuple, typename FeedbackTuple>
  inline std::shared_ptr<ActionSession<GoalTuple, FeedbackTuple>> 
  action_session_cast(std::shared_ptr<ActionSessionBase> base) {
    return std::dynamic_pointer_cast<ActionSession<GoalTuple, FeedbackTuple>>(base);
  }

} // namespace fins
