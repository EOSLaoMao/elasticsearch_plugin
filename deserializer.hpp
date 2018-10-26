#pragma once
#include <appbase/application.hpp>
#include <eosio/chain_plugin/chain_plugin.hpp>
#include <boost/thread/mutex.hpp>

#include "elastic_client.hpp"

namespace eosio {

class deserializer
{
public:
   deserializer(size_t size, fc::microseconds abi_serializer_max_time,
                const std::vector<std::string> url_list,
                const std::string &user, const std::string &password):
      abi_cache_size(size), abi_serializer_max_time(abi_serializer_max_time),
      es_client(url_list, user, password) {}

   template<typename T>
   fc::variant to_variant_with_abi( const T& obj ) {
      fc::variant pretty_output;
      abi_serializer::to_variant( obj, pretty_output,
                                  [&]( account_name n ) { return get_abi_serializer( n ); },
                                  abi_serializer_max_time );
      return pretty_output;
   }

   void erase_abi_cache(const account_name &name);

private:
   struct by_account;
   struct by_last_access;

   struct abi_cache {
      account_name                     account;
      fc::time_point                   last_accessed;
      fc::optional<abi_serializer>     serializer;
   };

   void purge_abi_cache();
   void insert_abi_cache( const abi_cache &entry );
   optional<abi_serializer> find_abi_cache(const account_name &name);
   optional<fc::variant> get_abi_by_account(const account_name &name);
   optional<abi_serializer> get_abi_serializer( const account_name &name );

   typedef boost::multi_index_container<abi_cache,
         indexed_by<
               ordered_unique< tag<by_account>,  member<abi_cache,account_name,&abi_cache::account> >,
               ordered_non_unique< tag<by_last_access>,  member<abi_cache,fc::time_point,&abi_cache::last_accessed> >
         >
   > abi_cache_index_t;

   size_t abi_cache_size = 0;
   abi_cache_index_t abi_cache_index;
   fc::microseconds abi_serializer_max_time;

   std::mutex client_mtx;
   std::mutex cache_mtx;

   elastic_client es_client;

};

}
