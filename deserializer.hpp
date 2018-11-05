#pragma once
#include <appbase/application.hpp>
#include <eosio/chain_plugin/chain_plugin.hpp>
#include <boost/thread/mutex.hpp>

#include "elastic_client.hpp"

namespace eosio {


class deserializer
{
public:
   deserializer(fc::microseconds abi_serializer_max_time): abi_serializer_max_time(abi_serializer_max_time) {}

   template<typename T>
   fc::variant to_variant_with_abi( const T& obj ) {
      fc::variant pretty_output;
      abi_serializer::to_variant( obj, pretty_output,
                                  [&]( account_name n ) { return get_abi_serializer( n ); },
                                  abi_serializer_max_time );
      return pretty_output;
   }

   void upsert_abi_cache( const account_name &name, const abi_def& abi );

private:
   struct by_account;

   struct abi_cache {
      account_name                     account;
      fc::optional<abi_serializer>     serializer;
   };

   optional<abi_serializer> get_abi_serializer( const account_name &name );

   typedef boost::multi_index_container<abi_cache,
         indexed_by<
               ordered_unique< tag<by_account>,  member<abi_cache,account_name,&abi_cache::account> >
         >
   > abi_cache_index_t;

   abi_cache_index_t abi_cache_index;
   fc::microseconds abi_serializer_max_time;

   boost::shared_mutex cache_mtx;
};

}
