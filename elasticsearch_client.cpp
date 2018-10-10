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

bool elasticsearch_client::head(const std::string &url_path)
{
   cpr::Response resp = client.performRequest(elasticlient::Client::HTTPMethod::HEAD, url_path, "");
   if ( resp.status_code == 200 ) {
      return true;
   } else if ( resp.status_code == 404 ) {
      return false;
   } else {
      EOS_THROW(chain::response_code_exception, "${code} ${text}", ("code", resp.status_code)("text", resp.text));
   }
}

bool elasticsearch_client::doc_exist(const std::string &index_name, const std::string &id)
{
   auto url = boost::str(boost::format("%1%/_doc/%2%") % index_name % id );
   return head(url);
}

void elasticsearch_client::index(const std::string &index_name, const std::string &body, const std::string &id)
{
   cpr::Response resp = client.index(index_name, "_doc", id, body);
   EOS_ASSERT(is_2xx(resp.status_code), chain::response_code_exception, "${code} ${text}", ("code", resp.status_code)("text", resp.text));
}

uint32_t elasticsearch_client::create(const std::string &index_name, const std::string &body, const std::string &id)
{
   auto url = boost::str(boost::format("%1%/_doc/%2%/_create") % index_name % id );
   cpr::Response resp = client.performRequest(elasticlient::Client::HTTPMethod::PUT, url, body);
   if ( (!is_2xx(resp.status_code)) && (resp.status_code != 409) )
      EOS_THROW(chain::response_code_exception, "${code} ${text}", ("code", resp.status_code)("text", resp.text));
   return resp.status_code;
}


void elasticsearch_client::init_index(const std::string &index_name, const std::string &mappings)
{
   if ( !head(index_name) ) {
      cpr::Response resp = client.performRequest(elasticlient::Client::HTTPMethod::PUT, index_name, mappings);
      EOS_ASSERT(is_2xx(resp.status_code), chain::response_code_exception, "${code} ${text}", ("code", resp.status_code)("text", resp.text));
   }
}

void elasticsearch_client::delete_index(const std::string &index_name)
{
   // retrn status code 404 if index not exists
   client.performRequest(elasticlient::Client::HTTPMethod::DELETE, index_name, "");
}

uint64_t elasticsearch_client::count_doc(const std::string &index_name, const std::string &query)
{
   auto url = boost::str(boost::format("%1%/_doc/_count") % index_name );
   cpr::Response resp = client.performRequest(elasticlient::Client::HTTPMethod::GET, url, query);
   EOS_ASSERT(is_2xx(resp.status_code), chain::response_code_exception, "${code} ${text}", ("code", resp.status_code)("text", resp.text));
   auto v = fc::json::from_string(resp.text);
   return v["count"].as_uint64();
}

bool elasticsearch_client::get(const std::string &index_name, const std::string &id, fc::variant &res)
{
   cpr::Response resp = client.get(index_name, "_doc", id);
   if ( !is_2xx(resp.status_code) ) return false;
   res = fc::json::from_string(resp.text);
   return true;
}

void elasticsearch_client::search(const std::string &index_name, fc::variant &v, const std::string &query)
{
   cpr::Response resp = client.search(index_name, "_doc", query);
   EOS_ASSERT(is_2xx(resp.status_code), chain::response_code_exception, "${code} ${text}", ("code", resp.status_code)("text", resp.text));
   v = fc::json::from_string(resp.text);
}

void elasticsearch_client::delete_by_query(const std::string &index_name, const std::string &query)
{
   auto url = boost::str(boost::format("%1%/_doc/_delete_by_query") % index_name );
   cpr::Response resp = client.performRequest(elasticlient::Client::HTTPMethod::POST, url, query);
   EOS_ASSERT(is_2xx(resp.status_code), chain::response_code_exception, "${code} ${text}", ("code", resp.status_code)("text", resp.text));
}

void elasticsearch_client::bulk_perform(elasticlient::SameIndexBulkData &bulk)
{
   auto index_name = bulk.indexName();
   auto body = bulk.body();
   auto url = boost::str(boost::format("%1%/_bulk") % index_name);
   cpr::Response resp = client.performRequest(elasticlient::Client::HTTPMethod::POST, url, body);
   EOS_ASSERT(is_2xx(resp.status_code), chain::response_code_exception, "${code} ${text}", ("code", resp.status_code)("text", resp.text));
   
   fc::variant text_doc( fc::json::from_string(resp.text) );
   EOS_ASSERT(text_doc["errors"].as_bool() == false, chain::bulk_fail_exception, "bulk perform errors: ${text}", ("text", resp.text));
}

void elasticsearch_client::update(const std::string &index_name, const std::string &id, const std::string &body)
{
   auto url = boost::str(boost::format("%1%/_doc/%2%/_update") % index_name % id);
   cpr::Response resp = client.performRequest(elasticlient::Client::HTTPMethod::POST, url, body);
   EOS_ASSERT(is_2xx(resp.status_code), chain::response_code_exception, "${code} ${text}", ("code", resp.status_code)("text", resp.text));
}

} // namespace eosio
