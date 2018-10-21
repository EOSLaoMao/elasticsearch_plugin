#pragma once
#include <vector>
#include <appbase/application.hpp>
#include <fc/variant.hpp>
#include <elasticlient/client.h>
#include <elasticlient/bulk.h>

namespace eosio {

class elastic_client
{
public:
   elastic_client(const std::vector<std::string> url_list, const std::string &user, const std::string &password)
      :client(url_list, user, password, 60000) {};

   void delete_index(const std::string &index_name);
   void init_index(const std::string &index_name, const std::string &mappings);
   bool head(const std::string &url_path);
   bool doc_exist(const std::string &index_name, const std::string &id);
   void get(const std::string &index_name, const std::string &id, fc::variant &res);
   void index(const std::string &index_name, const std::string &body, const std::string &id = std::string());
   uint32_t create(const std::string &index_name, const std::string &body, const std::string &id);
   uint64_t count_doc(const std::string &index_name, const std::string &query = std::string());
   void search(const std::string &index_name, fc::variant& v, const std::string &query);
   void delete_by_query(const std::string &index_name, const std::string &query);
   void bulk_perform(elasticlient::SameIndexBulkData &bulk);
   void bulk_perform(const std::string &bulk);
   void update(const std::string &index_name, const std::string &id, const std::string &body);

   std::string index_name;
   elasticlient::Client client;
};

}
