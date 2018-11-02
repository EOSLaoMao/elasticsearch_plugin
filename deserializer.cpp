#include <fc/variant.hpp>
#include "deserializer.hpp"
#include "exceptions.hpp"


namespace eosio
{

optional<abi_serializer> deserializer::get_abi_serializer(const account_name &name) {
   if( name.good()) {
      try {
         boost::shared_lock<boost::shared_mutex> lock(cache_mtx);
         auto itr = abi_cache_index.find( name );
         if( itr != abi_cache_index.end() ) {
            return itr->serializer;
         }
      } FC_CAPTURE_AND_LOG((name))
   }
   return optional<abi_serializer>();
}

void deserializer::upsert_abi_cache( const account_name &name, const abi_def& abi ) {
   if( name.good()) {
      try {
         boost::unique_lock<boost::shared_mutex> lock(cache_mtx);
         abi_cache_index.erase( name );
         abi_cache entry;
         entry.account = name;
         abi_serializer abis;

         if( name == chain::config::system_account_name ) {
            // redefine eosio setabi.abi from bytes to abi_def
            // Done so that abi is stored as abi_def in elasticsearch instead of as bytes
            abi_def abi_new = abi;
            auto itr = std::find_if( abi_new.structs.begin(), abi_new.structs.end(),
                                       []( const auto& s ) { return s.name == "setabi"; } );
            if( itr != abi_new.structs.end() ) {
               auto itr2 = std::find_if( itr->fields.begin(), itr->fields.end(),
                                          []( const auto& f ) { return f.name == "abi"; } );
               if( itr2 != itr->fields.end() ) {
                  if( itr2->type == "bytes" ) {
                     itr2->type = "abi_def";
                     // unpack setabi.abi as abi_def instead of as bytes
                     abis.add_specialized_unpack_pack( "abi_def",
                           std::make_pair<abi_serializer::unpack_function, abi_serializer::pack_function>(
                                 []( fc::datastream<const char*>& stream, bool is_array, bool is_optional ) -> fc::variant {
                                    EOS_ASSERT( !is_array && !is_optional, chain::elasticsearch_exception, "unexpected abi_def");
                                    chain::bytes temp;
                                    fc::raw::unpack( stream, temp );
                                    return fc::variant( fc::raw::unpack<abi_def>( temp ) );
                                 },
                                 []( const fc::variant& var, fc::datastream<char*>& ds, bool is_array, bool is_optional ) {
                                    EOS_ASSERT( false, chain::elasticsearch_exception, "never called" );
                                 }
                           ) );
                  }
               }
            }
            abis.set_abi( abi_new, abi_serializer_max_time );
         } else {
            abis.set_abi( abi, abi_serializer_max_time );
         }

         entry.serializer.emplace( std::move( abis ) );
         abi_cache_index.insert( entry );

         size_t size = abi_cache_index.size();
         if ( abi_cache_index.size() % 10000 == 0)
            ilog( "abi_cache_index size: ${s}", ("s", size) );

      } FC_CAPTURE_AND_LOG((name))
   }
}

}
