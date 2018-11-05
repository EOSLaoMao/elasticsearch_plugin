#pragma once
#include <mutex>

#include "elastic_client.hpp"

namespace eosio {

class bulker
{
public:

   bulker(size_t bulk_size,
          const std::vector<std::string> url_list,
          const std::string &user, const std::string &password):
      bulk_size(bulk_size), es_client(url_list, user, password), body(new std::string()) {}
   ~bulker();
   
   void append_document( std::string action, std::string source );

   size_t size();

private:
   size_t bulk_size = 0;
   size_t body_size = 0;

   void perform( std::unique_ptr<std::string> &&body );

   elastic_client es_client;
   std::unique_ptr<std::string> body;


   std::mutex client_mtx;
   std::mutex body_mtx;
};

class bulker_pool
{
public:
   bulker_pool(size_t size, size_t bulk_size,
               const std::vector<std::string> url_list,
               const std::string &user, const std::string &password);

   bulker& get();

private:
   std::vector<std::unique_ptr<bulker>> bulkers;
   size_t pool_size;
   size_t bulk_size;
   std::atomic<size_t> index {0};
};

}
