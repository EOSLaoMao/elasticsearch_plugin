#pragma once

#include <eosio/chain/exceptions.hpp>
#include <fc/exception/exception.hpp>
#include <appbase/application.hpp>
#include <boost/core/typeinfo.hpp>

namespace eosio {

namespace chain {

FC_DECLARE_DERIVED_EXCEPTION( elasticsearch_exception,    chain_exception,
                              3230004, "Elasticsearch exception" )
   FC_DECLARE_DERIVED_EXCEPTION( response_code_exception,   elasticsearch_exception,
                                 3230005, "Get non 2XX response code from Elasticsearch" )
   FC_DECLARE_DERIVED_EXCEPTION( bulk_fail_exception,       elasticsearch_exception,
                                 3230006, "Perform bulk get non zero errors" )
FC_DECLARE_DERIVED_EXCEPTION( bulkers_exception,    chain_exception,
                              3230007, "Bulkers exception" )
   FC_DECLARE_DERIVED_EXCEPTION( empty_bulker_pool_exception,   bulkers_exception,
                                 3230008, "Call get() on emtpy bulker pool" )
} 

inline void handle_elasticsearch_exception( const std::string& desc, int line_num ) {
   bool shutdown = true;
   try {
      try {
         throw;
      } catch( elasticlient::ConnectionException& e) {
         elog( "elasticsearch connection error, ${desc}, line ${line}, ${what}",
               ("desc", desc)( "line", line_num )( "what", e.what() ));
      } catch( chain::response_code_exception& e) {
         elog( "elasticsearch exception, ${desc}, line ${line}, ${what}",
               ("desc", desc)( "line", line_num )( "what", e.to_detail_string() ));
      } catch( chain::bulk_fail_exception& e) {
         wlog( "elasticsearch exception, ${desc}, line ${line}, ${what}",
               ("desc", desc)( "line", line_num )( "what", e.to_detail_string() ));
         shutdown = false;
       } catch( fc::exception& er ) {
         elog( "elasticsearch fc exception, ${desc}, line ${line}, ${details}",
               ("desc", desc)( "line", line_num )( "details", er.to_detail_string()));
      } catch( const std::exception& e ) {
         elog( "elasticsearch std exception, ${desc}, line ${line}, ${what}",
               ("desc", desc)( "line", line_num )( "what", e.what()));
      } catch( ... ) {
         elog( "elasticsearch unknown exception, ${desc}, line ${line_nun}", ("desc", desc)( "line_num", line_num ));
      }
   } catch (...) {
      std::cerr << "Exception attempting to handle exception for " << desc << " " << line_num << std::endl;
   }

   if( shutdown ) {
      // shutdown if elasticsearch failed to provide opportunity to fix issue and restart
      appbase::app().quit();
   }
}

} // eosio::chain
