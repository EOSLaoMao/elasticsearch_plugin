#pragma once
#include <eosio/chain/multi_index_includes.hpp>
#include <eosio/chain/database_utils.hpp>

using namespace eosio;
using namespace chainbase;

struct by_account;

class abi_cache: public chainbase::object<0, abi_cache> {
   OBJECT_CTOR(abi_cache,(abi))

   id_type                                id;
   account_name                           account;
   eosio::chain::shared_blob              abi;

   void set_abi( const eosio::chain::abi_def& a ) {
      abi.resize( fc::raw::pack_size( a ) );
      fc::datastream<char*> ds( abi.data(), abi.size() );
      fc::raw::pack( ds, a );
   }
};


using abi_cache_index_t = chainbase::shared_multi_index_container<abi_cache,
      indexed_by<
            ordered_unique< member<abi_cache,abi_cache::id_type,&abi_cache::id> >,
            ordered_unique< tag<by_account>,  member<abi_cache,account_name,&abi_cache::account> >
      >
>;

CHAINBASE_SET_INDEX_TYPE( abi_cache, abi_cache_index_t )


class serializer
{
public:
   serializer(const bfs::path& dir, fc::microseconds abi_serializer_max_time, uint64_t db_size)
      :db(dir, database::read_write, db_size), abi_serializer_max_time(abi_serializer_max_time)
   {
      db.add_index<abi_cache_index_t>();
   }

   optional<abi_serializer> get_abi_serializer( const account_name &name ) {
      if( name.good() ) {
         try {
            const auto& a = db.get<abi_cache, by_account>(name);
            abi_def abi;
            if( abi_serializer::to_abi( a.abi, abi ))
               return abi_def_to_serializer(name, abi);
         } catch( std::out_of_range& e) {
           // ignore missing abi exception.
         } FC_CAPTURE_AND_LOG((name))
      }
      return optional<abi_serializer>();
   }

   template<typename T>
   fc::variant to_variant_with_abi( const T& obj ) {
      fc::variant pretty_output;
      abi_serializer::to_variant( obj, pretty_output,
                                  [&]( account_name n ) { return get_abi_serializer( n ); },
                                  abi_serializer_max_time );
      return pretty_output;
   }

   void upsert_abi_cache( const account_name &name, const abi_def& abi ) {
      if( name.good()) {
         try {
            auto* a = db.find<abi_cache, by_account>(name);
            if ( a == nullptr ) {
               db.create<abi_cache>( [&]( abi_cache& ca ) {
                  ca.account = name;
                  ca.set_abi(abi);
               });
            } else {
               db.modify( *a, [&]( abi_cache& ca ) {
                  ca.set_abi(abi);
               });
            }
         } FC_CAPTURE_AND_LOG((name))
      }
   }

private:

   chainbase::database db;
   fc::microseconds abi_serializer_max_time;

   optional<abi_serializer> abi_def_to_serializer( const account_name &name, const abi_def& abi ) {
      if( name.good()) {
         try {
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

            return abis;
         } FC_CAPTURE_AND_LOG((name))
      }
      return optional<abi_serializer>();
   }

};
