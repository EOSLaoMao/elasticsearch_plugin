#include <cpr/response.h>

#include <fc/io/json.hpp>
#include <fc/log/logger.hpp>

#include <boost/format.hpp>

#include <eosio/chain/exceptions.hpp>

#include "elasticsearch_client.hpp"
#include "exceptions.hpp"

namespace eosio
{

namespace
{
bool is_2xx(int32_t status_code)
{
   return status_code > 199 && status_code < 300;
}
} // namespace

bool elasticsearch_client::index_exists()
{
   cpr::Response resp = client.performRequest(elasticlient::Client::HTTPMethod::HEAD, index_name, "");
   if ( is_2xx(resp.status_code) ) {
      return true;
   } else if ( resp.status_code == 404 ) {
      return false;
   } else {
      EOS_THROW(chain::response_code_exception, "${code} ${text}", ("code", resp.status_code)("text", resp.text));
   }
}

void elasticsearch_client::index(const std::string &body, const std::string &id)
{
   cpr::Response resp = client.index(index_name, "_doc", id, body);
   EOS_ASSERT(is_2xx(resp.status_code), chain::response_code_exception, "${code} ${text}", ("code", resp.status_code)("text", resp.text));
}

void elasticsearch_client::init_index(const std::string &mappings)
{
   if ( !index_exists() ) {
      cpr::Response resp = client.performRequest(elasticlient::Client::HTTPMethod::PUT, index_name, mappings);
      EOS_ASSERT(is_2xx(resp.status_code), chain::response_code_exception, "${code} ${text}", ("code", resp.status_code)("text", resp.text));
   }
}

void elasticsearch_client::delete_index()
{
   // retrn status code 404 if index not exists
   client.performRequest(elasticlient::Client::HTTPMethod::DELETE, index_name, "");
}

uint64_t elasticsearch_client::count_doc(const std::string &query)
{
   auto url = boost::str(boost::format("%1%/_doc/_count") % index_name );
   cpr::Response resp = client.performRequest(elasticlient::Client::HTTPMethod::GET, url, query);
   EOS_ASSERT(is_2xx(resp.status_code), chain::response_code_exception, "${code} ${text}", ("code", resp.status_code)("text", resp.text));
   auto v = fc::json::from_string(resp.text);
   return v["count"].as_uint64();
}

void elasticsearch_client::search(fc::variant &v, const std::string &query)
{
   cpr::Response resp = client.search(index_name, "_doc", query);
   EOS_ASSERT(is_2xx(resp.status_code), chain::response_code_exception, "${code} ${text}", ("code", resp.status_code)("text", resp.text));
   v = fc::json::from_string(resp.text);
}

void elasticsearch_client::delete_by_query(const std::string &query)
{
   auto url = boost::str(boost::format("%1%/_doc/_delete_by_query") % index_name );
   cpr::Response resp = client.performRequest(elasticlient::Client::HTTPMethod::POST, url, query);
   EOS_ASSERT(is_2xx(resp.status_code), chain::response_code_exception, "${code} ${text}", ("code", resp.status_code)("text", resp.text));
}

void elasticsearch_client::bulk_perform(elasticlient::SameIndexBulkData &bulk)
{
   size_t errors = bulk_indexer.perform(bulk);
   EOS_ASSERT(errors == 0, chain::bulk_fail_exception, "bulk perform error num: ${errors}", ("errors", errors));
}

void elasticsearch_client::update(const std::string &id, const std::string &doc)
{
   auto url = boost::str(boost::format("%1%/_doc/%2%/_update") % index_name % id);
   auto query = boost::str(boost::format(R"({ "doc": %1% })") % doc);
   cpr::Response resp = client.performRequest(elasticlient::Client::HTTPMethod::POST, url, query);
   EOS_ASSERT(is_2xx(resp.status_code), chain::response_code_exception, "${code} ${text}", ("code", resp.status_code)("text", resp.text));
}

} // namespace eosio
