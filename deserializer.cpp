#include "deserializer.hpp"
#include "exceptions.hpp"

#include <fc/variant.hpp>
#include <boost/thread/mutex.hpp>

namespace eosio
{

void deserializer::purge_abi_cache() {
   std::lock_guard<std::mutex> gurad(cache_mtx);
   if( abi_cache_index.size() < abi_cache_size ) return;

   // remove the oldest (smallest) last accessed
   auto& idx = abi_cache_index.get<by_last_access>();
   auto itr = idx.begin();
   if( itr != idx.end() ) {
      idx.erase( itr );
   }
}

optional<fc::variant> deserializer::get_abi_by_account(const account_name &name) {
   std::lock_guard<std::mutex> gurad(client_mtx);
   fc::variant res;
   try {
      es_client.get("accounts", std::to_string(name.value), res);
      return res["_source"]["abi"];
   } catch( elasticlient::ConnectionException& e) {
      elog( "elasticsearch connection error, line ${line}, ${what}",
            ( "line", __LINE__ )( "what", e.what() ));
   } catch( ... ) {
      // missing abi field
   }
   return optional<fc::variant>();
}

optional<abi_serializer> deserializer::find_abi_cache(const account_name &name) {
   std::lock_guard<std::mutex> gurad(cache_mtx);
   auto itr = abi_cache_index.find( name );
   if( itr != abi_cache_index.end() ) {
      abi_cache_index.modify( itr, []( auto& entry ) {
         entry.last_accessed = fc::time_point::now();
      });

      return itr->serializer;
   }
   return optional<abi_serializer>();
}

void deserializer::insert_abi_cache( const abi_cache &entry ) {
   std::lock_guard<std::mutex> gurad(cache_mtx);
   abi_cache_index.insert( entry );
}


void deserializer::erase_abi_cache(const account_name &name) {
   std::lock_guard<std::mutex> gurad(cache_mtx);
   abi_cache_index.erase( name );
}

optional<abi_serializer> deserializer::get_abi_serializer( const account_name &name ) {
   if( name.good()) {
      try {

         auto abi_opt = find_abi_cache(name);
         if ( abi_opt.valid() ) {
            return abi_opt;
         }

         auto abi_v = get_abi_by_account(name);
         if( abi_v.valid() ) {
            abi_def abi;
            try {
               abi = abi_v->as<abi_def>();
            } catch (...) {
               ilog( "Unable to convert account abi to abi_def for ${name}", ( "name", name ));
               return optional<abi_serializer>();
            }

            purge_abi_cache(); // make room if necessary
            abi_cache entry;
            entry.account = name;
            entry.last_accessed = fc::time_point::now();
            abi_serializer abis;
            if( name == chain::config::system_account_name ) {
               // redefine eosio setabi.abi from bytes to abi_def
               // Done so that abi is stored as abi_def in elasticsearch instead of as bytes
               auto itr = std::find_if( abi.structs.begin(), abi.structs.end(),
                                          []( const auto& s ) { return s.name == "setabi"; } );
               if( itr != abi.structs.end() ) {
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
            }
            abis.set_abi( abi, abi_serializer_max_time );
            entry.serializer.emplace( std::move( abis ) );
            insert_abi_cache( entry );
            return entry.serializer;
         }
      } FC_CAPTURE_AND_LOG((name))
   }
   return optional<abi_serializer>();
}

}
