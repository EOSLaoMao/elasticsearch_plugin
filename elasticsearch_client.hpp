#pragma once
#include <vector>
#include <appbase/application.hpp>
#include <fc/variant.hpp>
#include <elasticlient/client.h>
#include <elasticlient/bulk.h>

namespace eosio {

class elasticsearch_client
{
public:
   elasticsearch_client(const std::vector<std::string> url_list, const std::string &user, const std::string &password)
      :client(url_list, user, password), bulk_indexer(url_list, user, password){};

   void delete_index(const std::string &index_name);
   bool index_exists(const std::string &index_name);
   void init_index(const std::string &index_name, const std::string &mappings);
   bool get(const std::string &index_name, const std::string &id,  fc::variant &res);
   void index(const std::string &index_name, const std::string &body, const std::string &id = std::string());
   uint64_t count_doc(const std::string &index_name, const std::string &query = std::string());
   void search(const std::string &index_name, fc::variant& v, const std::string &query);
   void delete_by_query(const std::string &index_name, const std::string &query);
   void bulk_perform(const std::string &index_name, elasticlient::SameIndexBulkData &bulk);
   void update(const std::string &index_name, const std::string &id, const std::string &body);

   std::string index_name;
   elasticlient::Client client;
   elasticlient::Bulk bulk_indexer;
};

}
