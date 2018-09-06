/**
 *  @file
 *  @copyright defined in eos/LICENSE.txt
 */
#pragma once
#include <appbase/application.hpp>
#include <eosio/chain_plugin/chain_plugin.hpp> 

namespace eosio {

using namespace appbase;

/**
 *  This is a template plugin, intended to serve as a starting point for making new plugins
 */
class elasticsearch_plugin : public appbase::plugin<elasticsearch_plugin> {
public:
   APPBASE_PLUGIN_REQUIRES((chain_plugin))
   elasticsearch_plugin();
   virtual ~elasticsearch_plugin();
 
   virtual void set_program_options(options_description&, options_description& cfg) override;
 
   void plugin_initialize(const variables_map& options);
   void plugin_startup();
   void plugin_shutdown();

private:
   std::unique_ptr<class elasticsearch_plugin_impl> my;
};

}
