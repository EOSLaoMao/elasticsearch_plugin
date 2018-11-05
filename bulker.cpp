#include "bulker.hpp"
#include "exceptions.hpp"

namespace eosio {

bulker::~bulker() {
   ilog("draining bulker, size: ${n}", ("n", body_size));
   if ( !body->empty() ) {
      perform( std::move(body) );
   }
}

size_t bulker::size() {
   return body_size;
}

void bulker::perform( std::unique_ptr<std::string> &&body) {
   std::unique_ptr<std::string> bulk( std::move(body) );

   // dlog("bulk size: ${s}", ("s", bulk->size() ));

   std::lock_guard<std::mutex> guard(client_mtx);
   try {
      es_client.bulk_perform( *bulk );
   } catch (... ) {
      handle_elasticsearch_exception( "bulk exception", __LINE__ );
   }
}

void bulker::append_document( std::string action, std::string source ) {
   bool trigger = false;
   std::unique_ptr<std::string> temp( new std::string() );

   std::string doc( std::move(action) );
   doc.push_back('\n');
   doc.append( std::move(source) );
   doc.push_back('\n');

   {
      std::lock_guard<std::mutex> guard(body_mtx);
      body->append( doc );
      body_size = body->size();

      if ( body_size >= bulk_size ) {
         body.swap( temp );
         body_size = 0;
         trigger = true;
      }
   }

   if ( trigger ) {
      perform( std::move(temp) );
   }
}

bulker_pool::bulker_pool(size_t size, size_t bulk_size,
                         const std::vector<std::string> url_list,
                         const std::string &user, const std::string &password): pool_size(size), bulk_size(bulk_size)
{
   for (int i = 0; i < pool_size; ++i) {
      bulkers.emplace_back( new bulker(bulk_size, url_list, user, password) );
   }
}

bulker& bulker_pool::get() {
   if ( pool_size == 0 ) {
      EOS_THROW(chain::empty_bulker_pool_exception, "empty pool");
   }

   size_t cur_idx = index % pool_size;

   auto ptr = bulkers[cur_idx].get();

   if ( ptr->size() >= bulk_size ) {
      cur_idx = (cur_idx + 1) % pool_size;
      index = cur_idx;
      return *bulkers[cur_idx].get();
   } else {
      return *ptr;
   }
}

}
