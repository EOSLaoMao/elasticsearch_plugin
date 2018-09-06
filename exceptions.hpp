#pragma once

#include <fc/exception/exception.hpp>
#include <boost/core/typeinfo.hpp>

namespace eosio { namespace chain {

   FC_DECLARE_DERIVED_EXCEPTION( elasticsearch_exception,    chain_exception,
                                 3230004, "Elasticsearch exception" )
      FC_DECLARE_DERIVED_EXCEPTION( response_code_exception,   elasticsearch_exception,
                                    3230005, "Get non 2XX response code from Elasticsearch" )
      FC_DECLARE_DERIVED_EXCEPTION( bulk_fail_exception,       elasticsearch_exception,
                                    3230006, "Perform bulk get non zero errors" )
} } // eosio::chain
