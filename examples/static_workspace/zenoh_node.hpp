#pragma once

#include <atomic>
#include <chrono>
#include <cstring>
#include <fins/node.hpp>
#include <string>
#include <thread>
#include <zenoh.h>

namespace fins {

  class ZenohPublisherNode : public fins::Node {
  public:
    ZenohPublisherNode() {
      std::memset(&session_, 0, sizeof(session_));
      std::memset(&pub_, 0, sizeof(pub_));
    }

    void define() override {
      set_basics("ZenohPublisher", "Publishes a counter to Zenoh /hello_world", "Network");
      register_parameter<std::string>("key_expr", &ZenohPublisherNode::set_key, "/hello_world");
    }

    void initialize() override {
      if (connected_)
        return;

      z_owned_config_t config;
      z_config_default(&config);

      logger->info("Opening Zenoh session...");
      if (z_open(&session_, z_move(config), NULL) != Z_OK) {
        logger->error("Failed to open Zenoh session!");
        return;
      }

      connected_ = true;

      logger->info("Declaring Zenoh publisher on '{}'", key_expr_);

      z_owned_keyexpr_t ke;
      z_keyexpr_from_str(&ke, key_expr_.c_str());

      if (z_declare_publisher(z_loan(session_), &pub_, z_loan(ke), NULL) != Z_OK) {
        logger->error("Failed to declare Zenoh publisher!");
        z_drop(z_move(ke));
        cleanup_session();
        return;
      }

      pub_declared_ = true;

      z_drop(z_move(ke));
      logger->info("Zenoh Node initialized.");
    }

    void run() override {
      if (running_)
        return;
      running_ = true;

      worker_ = std::thread([this]() {
        int counter = 1;
        while (running_) {
          if (!pub_declared_) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            continue;
          }

          std::string val_str = std::to_string(counter++);

          z_publisher_put_options_t options;
          z_publisher_put_options_default(&options);

          z_owned_bytes_t payload;
          z_bytes_copy_from_buf(&payload, (const uint8_t *) val_str.c_str(), val_str.length());

          z_result_t res = z_publisher_put(z_loan(pub_), z_move(payload), &options);

          if (res == Z_OK) {
            logger->info("Zenoh Published: {} to {}", val_str, key_expr_);
          } else {
            logger->warn("Zenoh Put failed (Error: {})", res);
          }

          std::this_thread::sleep_for(std::chrono::seconds(1));
        }
      });
    }

    void pause() override {
      running_ = false;
      if (worker_.joinable())
        worker_.join();

      if (pub_declared_) {
        z_undeclare_publisher(z_move(pub_));
        pub_declared_ = false;
      }

      if (connected_) {
        cleanup_session();
      }

      logger->info("Zenoh Node paused and session closed.");
    }

    void reset() override { pause(); }

    void set_key(const std::string &key) { key_expr_ = key; }

  private:
    void cleanup_session() {
      if (!connected_)
        return;

      z_close((z_loaned_session_t *) z_loan(session_), NULL);

      z_drop(z_move(session_));

      connected_ = false;

      std::memset(&session_, 0, sizeof(session_));
    }

    std::string key_expr_ = "/hello_world";
    std::atomic<bool> running_{false};

    bool connected_ = false;
    bool pub_declared_ = false;

    std::thread worker_;

    z_owned_session_t session_;
    z_owned_publisher_t pub_;
  };

  EXPORT_NODE(ZenohPublisherNode)

} // namespace fins