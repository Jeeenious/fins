/*******************************************************************************
 * Copyright (c) 2025.
 * IWIN-FINS Lab, Shanghai Jiao Tong University, Shanghai, China.
 * All rights reserved.
 ******************************************************************************/

// action/action_manager.hpp

#pragma once

#include <fins/action/action_session.hpp>
#include <fins/action/action_tags.hpp>
#include <fins/action/action_traits.hpp>
#include <fins/macros.hpp>
#include <any>
#include <atomic>
#include <condition_variable>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <typeindex>

namespace fins {

  class FINS_API ActionManager {
  public:
    using GoalCallback = std::function<void(std::shared_ptr<ActionSessionBase>, const std::vector<std::any> &)>;
    using FeedbackCallback = std::function<void(const std::vector<std::any> &)>;
    using ResultCallback = std::function<void(ActionState)>;

    static ActionManager &get_instance();

    ActionManager(const ActionManager &) = delete;
    ActionManager &operator=(const ActionManager &) = delete;

    void register_commander(const std::string &action_name, std::type_index goal_type_id,
                            std::type_index feedback_type_id, ResultCallback result_cb, FeedbackCallback feedback_cb);

    void register_actor(const std::string &action_name, std::type_index goal_type_id,
                        std::type_index feedback_type_id, GoalCallback goal_cb);

    std::shared_ptr<ActionSessionBase> create_action_session(const std::string &action_name,
                                                              std::vector<std::any> goal_args,
                                                              std::type_index goal_type_id,
                                                              std::type_index feedback_type_id);

    void cancel_action(const std::string &action_name);

    ActionState get_action_state(const std::string &action_name);

  private:
    ActionManager();
    ~ActionManager();

    struct CommanderEntry {
      ResultCallback result_callback;
      FeedbackCallback feedback_callback;
      std::type_index goal_type_id = std::type_index(typeid(void));
      std::type_index feedback_type_id = std::type_index(typeid(void));
    };

    struct ActorEntry {
      GoalCallback goal_callback;
      std::type_index goal_type_id = std::type_index(typeid(void));
      std::type_index feedback_type_id = std::type_index(typeid(void));
      bool is_executing = false;
    };

    struct GoalTask {
      std::string action_name;
      std::vector<std::any> goal_args;
      std::type_index goal_type_id = std::type_index(typeid(void));
      std::type_index feedback_type_id = std::type_index(typeid(void));
    };

    void worker_loop();

  private:
    std::map<std::string, CommanderEntry> commanders_;
    std::map<std::string, ActorEntry> actors_;
    std::map<std::string, std::shared_ptr<ActionSessionBase>> active_sessions_;

    std::mutex map_mutex_;

    std::deque<GoalTask> goal_tasks_;
    std::mutex queue_mutex_;
    std::condition_variable cv_;

    std::thread worker_thread_;
    std::atomic<bool> stop_;
  };

} // namespace fins

#define FINS_ACTION_MANAGER ActionManager::get_instance()
