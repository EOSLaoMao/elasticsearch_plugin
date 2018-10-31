#include <eosio/elasticsearch_plugin/elasticsearch_plugin.hpp>
#include <eosio/chain/eosio_contract.hpp>
#include <eosio/chain/config.hpp>
#include <eosio/chain/exceptions.hpp>
#include <eosio/chain/transaction.hpp>
#include <eosio/chain/types.hpp>

#include <fc/io/json.hpp>
#include <fc/utf8.hpp>
#include <fc/variant.hpp>
#include <fc/variant_object.hpp>

#include <boost/asio.hpp>
#include <boost/chrono.hpp>
#include <boost/format.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/signals2/connection.hpp>
#include <boost/thread/thread.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/condition_variable.hpp>


#include <queue>
#include <stack>
#include <utility>
#include <unordered_map>

#include "elastic_client.hpp"
#include "exceptions.hpp"
#include "mappings.hpp"
#include "deserializer.hpp"
#include "bulker.hpp"
#include "ThreadPool/ThreadPool.h"


namespace eosio {

using chain::account_name;
using chain::action_name;
using chain::block_id_type;
using chain::permission_name;
using chain::transaction;
using chain::signed_transaction;
using chain::signed_block;
using chain::transaction_id_type;
using chain::packed_transaction;

static appbase::abstract_plugin& _elasticsearch_plugin = app().register_plugin<elasticsearch_plugin>();

struct filter_entry {
   name receiver;
   name action;
   name actor;

   friend bool operator<( const filter_entry& a, const filter_entry& b ) {
      return std::tie( a.receiver, a.action, a.actor ) < std::tie( b.receiver, b.action, b.actor );
   }

   //            receiver          action       actor
   bool match( const name& rr, const name& an, const name& ar ) const {
      return (receiver.value == 0 || receiver == rr) &&
             (action.value == 0 || action == an) &&
             (actor.value == 0 || actor == ar);
   }
};

class elasticsearch_plugin_impl {
public:
   elasticsearch_plugin_impl();
   ~elasticsearch_plugin_impl();

   fc::optional<boost::signals2::scoped_connection> accepted_block_connection;
   fc::optional<boost::signals2::scoped_connection> irreversible_block_connection;
   fc::optional<boost::signals2::scoped_connection> accepted_transaction_connection;
   fc::optional<boost::signals2::scoped_connection> applied_transaction_connection;

   void consume_blocks();

   void check_task_queue_size();

   void accepted_block( const chain::block_state_ptr& );
   void applied_irreversible_block(const chain::block_state_ptr&);
   void accepted_transaction(const chain::transaction_metadata_ptr&);
   void applied_transaction(const chain::transaction_trace_ptr&);

   void process_applied_transaction(chain::transaction_trace_ptr);
   void _process_applied_transaction(chain::transaction_trace_ptr);
   void process_accepted_transaction(chain::transaction_metadata_ptr);
   void _process_accepted_transaction(chain::transaction_metadata_ptr);
   void process_accepted_block( chain::block_state_ptr );
   void _process_accepted_block( chain::block_state_ptr );
   void process_irreversible_block( chain::block_state_ptr );
   void _process_irreversible_block( chain::block_state_ptr );

   void upsert_account(
         std::unordered_map<uint64_t, std::pair<std::string, fc::mutable_variant_object>> &account_upsert_actions,
         const chain::action& act, const chain::block_timestamp_type& block_time );
   void create_new_account( fc::mutable_variant_object& param_doc, const chain::newaccount& newacc, std::chrono::milliseconds& now,
         const chain::block_timestamp_type& block_time );
   void update_account_auth( fc::mutable_variant_object& param_doc, const chain::updateauth& update, std::chrono::milliseconds& now );
   void delete_account_auth( fc::mutable_variant_object& param_doc, const chain::deleteauth& del, std::chrono::milliseconds& now );
   void upsert_account_setabi( fc::mutable_variant_object& param_doc, const chain::setabi& setabi, std::chrono::milliseconds& now );

   /// @return true if act should be added to elasticsearch, false to skip it
   bool filter_include( const account_name& receiver, const action_name& act_name,
                        const vector<chain::permission_level>& authorization ) const;
   bool filter_include( const transaction& trx ) const;

   void init();
   void delete_index();

   template<typename Queue, typename Entry> void queue(Queue& queue, const Entry& e);

   bool configured{false};
   bool delete_index_on_startup{false};
   uint32_t start_block_num = 0;
   std::atomic_bool start_block_reached{false};

   bool filter_on_star = true;
   std::set<filter_entry> filter_on;
   std::set<filter_entry> filter_out;
   bool store_blocks = true;
   bool store_block_states = true;
   bool store_transactions = true;
   bool store_transaction_traces = true;
   bool store_action_traces = true;

   std::unique_ptr<elastic_client> es_client;
   std::unique_ptr<deserializer> abi_deserializer;
   std::unique_ptr<bulker_pool> bulk_pool;
   std::unique_ptr<ThreadPool> thread_pool;
   size_t max_task_queue_size = 0;
   int task_queue_sleep_time = 0;

   size_t max_queue_size = 0;
   int queue_sleep_time = 0;
   size_t abi_cache_size = 0;
   std::deque<chain::transaction_metadata_ptr> transaction_metadata_queue;
   std::deque<chain::transaction_metadata_ptr> transaction_metadata_process_queue;
   std::deque<chain::transaction_trace_ptr> transaction_trace_queue;
   std::deque<chain::transaction_trace_ptr> transaction_trace_process_queue;
   std::deque<chain::block_state_ptr> block_state_queue;
   std::deque<chain::block_state_ptr> block_state_process_queue;
   std::deque<chain::block_state_ptr> irreversible_block_state_queue;
   std::deque<chain::block_state_ptr> irreversible_block_state_process_queue;
   boost::mutex mtx;
   boost::condition_variable condition;
   boost::thread consume_thread;
   boost::atomic<bool> done{false};
   boost::atomic<bool> startup{true};
   fc::optional<chain::chain_id_type> chain_id;
   fc::microseconds abi_serializer_max_time;

   static const action_name newaccount;
   static const action_name setabi;
   static const action_name updateauth;
   static const action_name deleteauth;
   static const permission_name owner;
   static const permission_name active;

   static const std::string accounts_index;
   static const std::string blocks_index;
   static const std::string trans_index;
   static const std::string block_states_index;
   static const std::string trans_traces_index;
   static const std::string action_traces_index;
};

const action_name elasticsearch_plugin_impl::newaccount = chain::newaccount::get_name();
const action_name elasticsearch_plugin_impl::setabi = chain::setabi::get_name();
const action_name elasticsearch_plugin_impl::updateauth = chain::updateauth::get_name();
const action_name elasticsearch_plugin_impl::deleteauth = chain::deleteauth::get_name();
const permission_name elasticsearch_plugin_impl::owner = chain::config::owner_name;
const permission_name elasticsearch_plugin_impl::active = chain::config::active_name;

const std::string elasticsearch_plugin_impl::accounts_index = "accounts";
const std::string elasticsearch_plugin_impl::blocks_index = "blocks";
const std::string elasticsearch_plugin_impl::trans_index = "transactions";
const std::string elasticsearch_plugin_impl::block_states_index = "block_states";
const std::string elasticsearch_plugin_impl::trans_traces_index = "transaction_traces";
const std::string elasticsearch_plugin_impl::action_traces_index = "action_traces";

bool elasticsearch_plugin_impl::filter_include( const account_name& receiver, const action_name& act_name,
                                           const vector<chain::permission_level>& authorization ) const
{
   bool include = false;
   if( filter_on_star ) {
      include = true;
   } else {
      auto itr = std::find_if( filter_on.cbegin(), filter_on.cend(), [&receiver, &act_name]( const auto& filter ) {
         return filter.match( receiver, act_name, 0 );
      } );
      if( itr != filter_on.cend() ) {
         include = true;
      } else {
         for( const auto& a : authorization ) {
            auto itr = std::find_if( filter_on.cbegin(), filter_on.cend(), [&receiver, &act_name, &a]( const auto& filter ) {
               return filter.match( receiver, act_name, a.actor );
            } );
            if( itr != filter_on.cend() ) {
               include = true;
               break;
            }
         }
      }
   }

   if( !include ) { return false; }
   if( filter_out.empty() ) { return true; }

   auto itr = std::find_if( filter_out.cbegin(), filter_out.cend(), [&receiver, &act_name]( const auto& filter ) {
      return filter.match( receiver, act_name, 0 );
   } );
   if( itr != filter_out.cend() ) { return false; }

   for( const auto& a : authorization ) {
      auto itr = std::find_if( filter_out.cbegin(), filter_out.cend(), [&receiver, &act_name, &a]( const auto& filter ) {
         return filter.match( receiver, act_name, a.actor );
      } );
      if( itr != filter_out.cend() ) { return false; }
   }

   return true;
}

bool elasticsearch_plugin_impl::filter_include( const transaction& trx ) const
{
   if( !filter_on_star || !filter_out.empty() ) {
      bool include = false;
      for( const auto& a : trx.actions ) {
         if( filter_include( a.account, a.name, a.authorization ) ) {
            include = true;
            break;
         }
      }
      if( !include ) {
         for( const auto& a : trx.context_free_actions ) {
            if( filter_include( a.account, a.name, a.authorization ) ) {
               include = true;
               break;
            }
         }
      }
      return include;
   }
   return true;
}

elasticsearch_plugin_impl::elasticsearch_plugin_impl()
{
}

elasticsearch_plugin_impl::~elasticsearch_plugin_impl()
{
   if (!startup) {
      try {
         ilog( "elasticsearch_plugin shutdown in process please be patient this can take a few minutes" );
         done = true;
         condition.notify_one();

         consume_thread.join();
      } catch( std::exception& e ) {
         elog( "Exception on elasticsearch_plugin shutdown of consume thread: ${e}", ("e", e.what()));
      }
   }
}

template<typename Queue, typename Entry>
void elasticsearch_plugin_impl::queue( Queue& queue, const Entry& e ) {
   boost::mutex::scoped_lock lock( mtx );
   auto queue_size = queue.size();
   if( queue_size > max_queue_size ) {
      lock.unlock();
      condition.notify_one();
      queue_sleep_time += 10;
      if( queue_sleep_time > 1000 )
         wlog("queue size: ${q}", ("q", queue_size));
      boost::this_thread::sleep_for( boost::chrono::milliseconds( queue_sleep_time ));
      lock.lock();
   } else {
      queue_sleep_time -= 10;
      if( queue_sleep_time < 0 ) queue_sleep_time = 0;
   }
   queue.emplace_back( e );
   lock.unlock();
   condition.notify_one();
}

void elasticsearch_plugin_impl::accepted_transaction( const chain::transaction_metadata_ptr& t ) {
   try {
      if( store_transactions ) {
         queue( transaction_metadata_queue, t );
      }
   } catch (fc::exception& e) {
      elog("FC Exception while accepted_transaction ${e}", ("e", e.to_string()));
   } catch (std::exception& e) {
      elog("STD Exception while accepted_transaction ${e}", ("e", e.what()));
   } catch (...) {
      elog("Unknown exception while accepted_transaction");
   }
}

void elasticsearch_plugin_impl::applied_transaction( const chain::transaction_trace_ptr& t ) {
   try {
      // Traces emitted from an incomplete block leave the producer_block_id as empty.
      //
      // Avoid adding the action traces or transaction traces to the database if the producer_block_id is empty.
      // This way traces from speculatively executed transactions are not included in the Elasticsearch which can
      // avoid potential confusion for consumers of that database.
      //
      // Due to forks, it could be possible for multiple incompatible action traces with the same block_num and trx_id
      // to exist in the database. And if the producer double produces a block, even the block_time may not
      // disambiguate the two action traces. Without a producer_block_id to disambiguate and determine if the action
      // trace comes from an orphaned fork branching off of the blockchain, consumers of the Mongo DB database may be
      // reacting to a stale action trace that never actually executed in the current blockchain.
      //
      // It is better to avoid this potential confusion by not logging traces from speculative execution, i.e. emitted
      // from an incomplete block. This means that traces will not be recorded in speculative read-mode, but
      // users should not be using the elasticsearch_plugin in that mode anyway.
      //
      // Allow logging traces if node is a producer for testing purposes, so a single nodeos can do both for testing.
      //
      // It is recommended to run elasticsearch_plugin in read-mode = read-only.
      //
      if( !t->producer_block_id.valid() )
         return;
      // always queue since account information always gathered
      queue( transaction_trace_queue, t );
   } catch (fc::exception& e) {
      elog("FC Exception while applied_transaction ${e}", ("e", e.to_string()));
   } catch (std::exception& e) {
      elog("STD Exception while applied_transaction ${e}", ("e", e.what()));
   } catch (...) {
      elog("Unknown exception while applied_transaction");
   }
}

void elasticsearch_plugin_impl::applied_irreversible_block( const chain::block_state_ptr& bs ) {
   try {
      if( store_blocks || store_block_states || store_transactions ) {
         queue( irreversible_block_state_queue, bs );
      }
   } catch (fc::exception& e) {
      elog("FC Exception while applied_irreversible_block ${e}", ("e", e.to_string()));
   } catch (std::exception& e) {
      elog("STD Exception while applied_irreversible_block ${e}", ("e", e.what()));
   } catch (...) {
      elog("Unknown exception while applied_irreversible_block");
   }
}

void elasticsearch_plugin_impl::accepted_block( const chain::block_state_ptr& bs ) {
   try {
      if( !start_block_reached ) {
         if( bs->block_num >= start_block_num ) {
            start_block_reached = true;
         }
      }
      if( store_blocks || store_block_states ) {
         queue( block_state_queue, bs );
      }
   } catch (fc::exception& e) {
      elog("FC Exception while accepted_block ${e}", ("e", e.to_string()));
   } catch (std::exception& e) {
      elog("STD Exception while accepted_block ${e}", ("e", e.what()));
   } catch (...) {
      elog("Unknown exception while accepted_block");
   }
}

void elasticsearch_plugin_impl::process_accepted_transaction( chain::transaction_metadata_ptr t ) {
   try {
      if( start_block_reached ) {
         _process_accepted_transaction( std::move(t) );
      }
   } catch (fc::exception& e) {
      elog("FC Exception while processing accepted transaction metadata: ${e}", ("e", e.to_detail_string()));
   } catch (std::exception& e) {
      elog("STD Exception while processing accepted tranasction metadata: ${e}", ("e", e.what()));
   } catch (...) {
      elog("Unknown exception while processing accepted transaction metadata");
   }
}

void elasticsearch_plugin_impl::process_applied_transaction( chain::transaction_trace_ptr t ) {
   try {
      // always call since we need to capture setabi on accounts even if not storing transaction traces
      _process_applied_transaction( std::move(t) );
   } catch (fc::exception& e) {
      elog("FC Exception while processing applied transaction trace: ${e}", ("e", e.to_detail_string()));
   } catch (std::exception& e) {
      elog("STD Exception while processing applied transaction trace: ${e}", ("e", e.what()));
   } catch (...) {
      elog("Unknown exception while processing applied transaction trace");
   }
}

void elasticsearch_plugin_impl::process_irreversible_block( chain::block_state_ptr bs) {
  try {
     if( start_block_reached ) {
        _process_irreversible_block( std::move(bs) );
     }
  } catch (fc::exception& e) {
     elog("FC Exception while processing irreversible block: ${e}", ("e", e.to_detail_string()));
  } catch (std::exception& e) {
     elog("STD Exception while processing irreversible block: ${e}", ("e", e.what()));
  } catch (...) {
     elog("Unknown exception while processing irreversible block");
  }
}

void elasticsearch_plugin_impl::process_accepted_block( chain::block_state_ptr bs ) {
   try {
      if( start_block_reached ) {
         _process_accepted_block( std::move(bs) );
      }
   } catch (fc::exception& e) {
      elog("FC Exception while processing accepted block trace ${e}", ("e", e.to_string()));
   } catch (std::exception& e) {
      elog("STD Exception while processing accepted block trace ${e}", ("e", e.what()));
   } catch (...) {
      elog("Unknown exception while processing accepted block trace");
   }
}

void elasticsearch_plugin_impl::create_new_account(
   fc::mutable_variant_object& param_doc, const chain::newaccount& newacc, std::chrono::milliseconds& now,
   const chain::block_timestamp_type& block_time )
{
   fc::variants pub_keys;
   fc::variants account_controls;

   param_doc("name", newacc.name.to_string());
   param_doc("creator", newacc.creator.to_string());
   param_doc("account_create_time", block_time);
   param_doc("createAt", now.count());

   for( const auto& account : newacc.owner.accounts ) {
      fc::mutable_variant_object account_entry;
      account_entry( "permission", owner.to_string());
      account_entry( "name", account.permission.actor.to_string());
      account_controls.emplace_back(account_entry);
   }

   for( const auto& account : newacc.active.accounts ) {
      fc::mutable_variant_object account_entry;
      account_entry( "permission", active.to_string());
      account_entry( "name", account.permission.actor.to_string());
      account_controls.emplace_back(account_entry);
   }

   for( const auto& pub_key_weight : newacc.owner.keys ) {
      fc::mutable_variant_object key_entry;
      key_entry( "permission", owner.to_string());
      key_entry( "key", pub_key_weight.key.operator string());
      pub_keys.emplace_back(key_entry);
   }

   for( const auto& pub_key_weight : newacc.active.keys ) {
      fc::mutable_variant_object key_entry;
      key_entry( "permission", active.to_string());
      key_entry( "key", pub_key_weight.key.operator string());
      pub_keys.emplace_back(key_entry);
   }

   param_doc("pub_keys", pub_keys);
   param_doc("account_controls", account_controls);
}

void elasticsearch_plugin_impl::update_account_auth(
   fc::mutable_variant_object& param_doc, const chain::updateauth& update, std::chrono::milliseconds& now )
{
   fc::variants pub_keys;
   fc::variants account_controls;

   for( const auto& pub_key_weight : update.auth.keys ) {
      fc::mutable_variant_object key_entry;
      key_entry( "permission", update.permission.to_string());
      key_entry( "key", pub_key_weight.key.operator string());
      pub_keys.emplace_back(key_entry);
   }

   for( const auto& account : update.auth.accounts ) {
      fc::mutable_variant_object account_entry;
      account_entry( "permission", update.permission.to_string());
      account_entry( "name", account.permission.actor.to_string());
      account_controls.emplace_back(account_entry);
   }

   param_doc("permission", update.permission.to_string());
   param_doc("pub_keys", pub_keys);
   param_doc("account_controls", account_controls);
   param_doc("updateAt", now.count());
}

void elasticsearch_plugin_impl::delete_account_auth(
   fc::mutable_variant_object& param_doc, const chain::deleteauth& del, std::chrono::milliseconds& now )
{
   param_doc("permission", del.permission.to_string());
   param_doc("updateAt", now.count());
}

void elasticsearch_plugin_impl::upsert_account_setabi(
   fc::mutable_variant_object& param_doc, const chain::setabi& setabi, std::chrono::milliseconds& now )
{
   abi_def abi_def = fc::raw::unpack<chain::abi_def>( setabi.abi );

   param_doc("name", setabi.account.to_string());
   param_doc("abi", abi_def);
   param_doc("updateAt", now.count());
}

void elasticsearch_plugin_impl::upsert_account(
      std::unordered_map<uint64_t, std::pair<std::string, fc::mutable_variant_object>> &account_upsert_actions,
      const chain::action& act, const chain::block_timestamp_type& block_time )
{
   if (act.account != chain::config::system_account_name)
      return;

   std::chrono::milliseconds now = std::chrono::duration_cast<std::chrono::milliseconds>(
         std::chrono::microseconds{fc::time_point::now().time_since_epoch().count()} );

   uint64_t account_id;
   std::string upsert_script;
   fc::mutable_variant_object param_doc;

   try {
      if( act.name == newaccount ) {
         auto newacc = act.data_as<chain::newaccount>();

         create_new_account(param_doc, newacc, now, block_time);
         account_id = newacc.name.value;
         upsert_script =
            "ctx._source.name = params[\"%1%\"].name;"
            "ctx._source.creator = params[\"%1%\"].creator;"
            "ctx._source.account_create_time = params[\"%1%\"].account_create_time;"
            "ctx._source.pub_keys = params[\"%1%\"].pub_keys;"
            "ctx._source.account_controls = params[\"%1%\"].account_controls;"
            "ctx._source.createAt = params[\"%1%\"].createAt;";

      } else if( act.name == updateauth ) {
         const auto update = act.data_as<chain::updateauth>();

         update_account_auth(param_doc, update, now);
         account_id = update.account.value;
         upsert_script =
            "ctx._source.pub_keys.removeIf(item -> item.permission == params[\"%1%\"].permission);"
            "ctx._source.account_controls.removeIf(item -> item.permission == params[\"%1%\"].permission);"
            "ctx._source.pub_keys.addAll(params[\"%1%\"].pub_keys);"
            "ctx._source.account_controls.addAll(params[\"%1%\"].account_controls);"
            "ctx._source.updateAt = params[\"%1%\"].updateAt;";

      } else if( act.name == deleteauth ) {
         const auto del = act.data_as<chain::deleteauth>();

         delete_account_auth(param_doc, del, now);
         account_id = del.account.value;
         upsert_script =
            "ctx._source.pub_keys.removeIf(item -> item.permission == params[\"%1%\"].permission);"
            "ctx._source.account_controls.removeIf(item -> item.permission == params[\"%1%\"].permission);"
            "ctx._source.updateAt = params[\"%1%\"].updateAt;";

      } else if( act.name == setabi ) {
         auto setabi = act.data_as<chain::setabi>();

         abi_deserializer->erase_abi_cache( setabi.account );

         upsert_account_setabi(param_doc, setabi, now);
         account_id = setabi.account.value;
         upsert_script =
            "ctx._source.name = params[\"%1%\"].name;"
            "ctx._source.abi = params[\"%1%\"].abi;"
            "ctx._source.updateAt = params[\"%1%\"].updateAt;";
      }

      if ( !upsert_script.empty() ) {
         auto it = account_upsert_actions.find(account_id);
         if ( it != account_upsert_actions.end() ) {
            auto idx = std::to_string(it->second.second.size());
            auto script = boost::str(boost::format(upsert_script) % idx);
            it->second.first.append(script);
            it->second.second.operator()(idx, param_doc);
         } else {
            auto idx = "0";
            auto script = boost::str(boost::format(upsert_script) % idx);
            account_upsert_actions.emplace(
               account_id,
               std::pair<std::string, fc::mutable_variant_object>(script, fc::mutable_variant_object(idx, param_doc)));
         }
      }

   } catch( fc::exception& e ) {
      // if unable to unpack native type, skip account creation
   }
}

void elasticsearch_plugin_impl::_process_applied_transaction( chain::transaction_trace_ptr t ) {

   elasticlient::SameIndexBulkData bulk_account_upserts(accounts_index);

   std::unordered_map<uint64_t, std::pair<std::string, fc::mutable_variant_object>> account_upsert_actions;
   std::vector<std::pair<uint64_t, std::reference_wrapper<chain::base_action_trace>>> base_action_traces; // without inline action traces

   bool executed = t->receipt.valid() && t->receipt->status == chain::transaction_receipt_header::executed;

   uint64_t action_count = 0;
   std::stack<std::reference_wrapper<chain::action_trace>> stack;
   for( auto& atrace : t->action_traces ) {
      stack.emplace(atrace);

      while ( !stack.empty() )
      {
         auto &atrace = stack.top().get();
         stack.pop();

         if( executed && atrace.receipt.receiver == chain::config::system_account_name ) {
            upsert_account( account_upsert_actions, atrace.act, atrace.block_time );
         }

         if( start_block_reached && store_action_traces
            && filter_include( atrace.receipt.receiver, atrace.act.name, atrace.act.authorization ) ) {
            base_action_traces.emplace_back(std::make_pair(action_count, std::ref(atrace)));
         }
         action_count++;

         auto &inline_traces = atrace.inline_traces;
         for( auto it = inline_traces.rbegin(); it != inline_traces.rend(); ++it ) {
            stack.emplace(*it);
         }
      }
   }

   for( auto& action : account_upsert_actions ) {

      fc::mutable_variant_object source_doc;
      fc::mutable_variant_object script_doc;

      script_doc("lang", "painless");
      script_doc("source", action.second.first);
      script_doc("params", action.second.second);

      source_doc("scripted_upsert", true);
      source_doc("upsert", fc::variant_object());
      source_doc("script", script_doc);

      auto id = std::to_string(action.first);
      auto json = fc::json::to_string(source_doc);

      bulk_account_upserts.updateDocument("_doc", id, json);
   }

   if ( !bulk_account_upserts.empty() ) {
      try {
         es_client->bulk_perform(bulk_account_upserts);
      } catch( ... ) {
         handle_elasticsearch_exception( "upsert accounts " + bulk_account_upserts.body(), __LINE__ );
      }
   }

   if( base_action_traces.empty() ) return; //< do not index transaction_trace if all action_traces filtered out
   check_task_queue_size();
   thread_pool->enqueue(
      [ t{std::move(t)}, base_action_traces{std::move(base_action_traces)}, this ]()
      {
         const auto& trx_id = t->id;
         const auto trx_id_str = trx_id.str();
         auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::microseconds{fc::time_point::now().time_since_epoch().count()});

         for (auto& p : base_action_traces) {
            fc::mutable_variant_object action_traces_doc;
            chain::base_action_trace &base = p.second.get();
            fc::from_variant( abi_deserializer->to_variant_with_abi( base ), action_traces_doc );
            action_traces_doc("createAt", now.count());

            auto id = boost::str(boost::format("%1%-%2%") % trx_id_str % p.first);

            fc::mutable_variant_object action_doc;
            action_doc("_index", action_traces_index);
            action_doc("_type", "_doc");
            action_doc("_id", id);
            action_doc("retry_on_conflict", 100);

            auto action = fc::json::to_string( fc::variant_object("index", action_doc) );
            auto json = fc::prune_invalid_utf8( fc::json::to_string(action_traces_doc) );

            bulker& bulk = bulk_pool->get();
            bulk.append_document(std::move(action), std::move(json));
         }

         if( start_block_reached && store_transaction_traces ) {
            // transaction trace index

            fc::mutable_variant_object trans_traces_doc;
            fc::from_variant( abi_deserializer->to_variant_with_abi( *t ), trans_traces_doc );
            trans_traces_doc("createAt", now.count());

            fc::mutable_variant_object action_doc;
            action_doc("_index", trans_traces_index);
            action_doc("_type", "_doc");
            action_doc("_id", trx_id_str);
            action_doc("retry_on_conflict", 100);

            auto action = fc::json::to_string( fc::variant_object("index", action_doc) );
            auto json = fc::prune_invalid_utf8( fc::json::to_string( trans_traces_doc ) );

            bulker& bulk = bulk_pool->get();
            bulk.append_document(std::move(action), std::move(json));

         }
      }
   );

}

void elasticsearch_plugin_impl::_process_accepted_transaction( chain::transaction_metadata_ptr t ) {
   check_task_queue_size();
   thread_pool->enqueue(
      [ t{std::move(t)}, this ]()
      {
         const auto& trx = t->trx;
         if( !filter_include( trx ) ) return;

         const auto& trx_id = t->id;
         const auto trx_id_str = trx_id.str();

         fc::mutable_variant_object trans_doc;
         fc::mutable_variant_object doc;

         auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::microseconds{fc::time_point::now().time_since_epoch().count()} );

         fc::from_variant( abi_deserializer->to_variant_with_abi( trx ), trans_doc );
         trans_doc("trx_id", trx_id_str);

         fc::variant signing_keys;
         if( t->signing_keys.valid() ) {
            signing_keys = t->signing_keys->second;
         } else {
            signing_keys = trx.get_signature_keys( *chain_id, false, false );
         }

         if( !signing_keys.is_null() ) {
            trans_doc("signing_keys", signing_keys);
         }

         trans_doc("accepted", t->accepted);
         trans_doc("implicit", t->implicit);
         trans_doc("scheduled", t->scheduled);
         trans_doc("createAt", now.count());

         doc("doc", trans_doc);
         doc("doc_as_upsert", true);

         fc::mutable_variant_object action_doc;
         action_doc("_index", trans_index);
         action_doc("_type", "_doc");
         action_doc("_id", trx_id_str);
         action_doc("retry_on_conflict", 100);

         auto action = fc::json::to_string( fc::variant_object("update", action_doc) );
         auto json = fc::prune_invalid_utf8( fc::json::to_string( doc ) );

         bulker& bulk = bulk_pool->get();
         bulk.append_document(std::move(action), std::move(json));
      }
   );
}

void elasticsearch_plugin_impl::_process_accepted_block( chain::block_state_ptr bs ) {
   check_task_queue_size();
   thread_pool->enqueue(
      [ bs{std::move(bs)}, this ]()
      {
         auto block_num = bs->block_num;
         if( block_num % 1000 == 0 )
            ilog( "block_num: ${b}", ("b", block_num) );

         const auto block_id = bs->id;
         const auto block_id_str = block_id.str();

         auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::microseconds{fc::time_point::now().time_since_epoch().count()});

         if( store_block_states ) {
            auto source =
                  "if (!ctx._source.containsKey(\"block_num\")) ctx._source.block_num = params.block_num;"
                  "if (!ctx._source.containsKey(\"block_id\")) ctx._source.block_id = params.block_id;"
                  "if (!ctx._source.containsKey(\"validated\")) ctx._source.validated = params.validated;"
                  "if (!ctx._source.containsKey(\"block_header_state\")) ctx._source.block_header_state = params.block_header_state;"
                  "ctx._source.createAt = params.createAt;";

            fc::mutable_variant_object doc;
            fc::mutable_variant_object params_doc;
            fc::mutable_variant_object script_doc;

            params_doc("block_num", static_cast<int32_t>(block_num));
            params_doc("block_id", block_id_str);
            params_doc("validated", bs->validated);
            params_doc("block_header_state", bs);
            params_doc("createAt", now.count());

            script_doc("source", source);
            script_doc("lang", "painless");
            script_doc("params", params_doc);

            doc("script", script_doc);
            doc("scripted_upsert", true);
            doc("upsert", fc::variant_object());

            fc::mutable_variant_object action_doc;
            action_doc("_index", block_states_index);
            action_doc("_type", "_doc");
            action_doc("_id", block_id_str);
            action_doc("retry_on_conflict", 100);

            auto action = fc::json::to_string( fc::variant_object("update", action_doc) );
            auto json = fc::json::to_string( doc );

            bulker& bulk = bulk_pool->get();
            bulk.append_document(std::move(action), std::move(json));
         }

         if( store_blocks ) {
            auto source =
               "if (!ctx._source.containsKey(\"block_num\")) ctx._source.block_num = params.block_num;"
               "if (!ctx._source.containsKey(\"block_id\")) ctx._source.block_id = params.block_id;"
               "if (!ctx._source.containsKey(\"block\")) ctx._source.block = params.block;"
               "if (!ctx._source.containsKey(\"irreversible\")) ctx._source.irreversible = params.irreversible;"
               "ctx._source.createAt = params.createAt;";

            fc::mutable_variant_object doc;
            fc::mutable_variant_object params_doc;
            fc::mutable_variant_object script_doc;

            params_doc("block_num", static_cast<int32_t>(block_num));
            params_doc("block_id", block_id_str);
            params_doc("block", abi_deserializer->to_variant_with_abi( *bs->block ));
            params_doc("irreversible", false);
            params_doc("createAt", now.count());

            script_doc("source", source);
            script_doc("lang", "painless");
            script_doc("params", params_doc);

            doc("script", script_doc);
            doc("scripted_upsert", true);
            doc("upsert", fc::variant_object());

            fc::mutable_variant_object action_doc;
            action_doc("_index", blocks_index);
            action_doc("_type", "_doc");
            action_doc("_id", block_id_str);
            action_doc("retry_on_conflict", 100);


            auto action = fc::json::to_string( fc::variant_object("update", action_doc) );
            auto json = fc::prune_invalid_utf8( fc::json::to_string( doc ) );

            bulker& bulk = bulk_pool->get();
            bulk.append_document(std::move(action), std::move(json));
         }
      }
   );
}

void elasticsearch_plugin_impl::_process_irreversible_block(chain::block_state_ptr bs) {
   check_task_queue_size();
   thread_pool->enqueue(
      [ bs{std::move(bs)}, this ]()
      {
         const auto block_id = bs->block->id();
         const auto block_id_str = block_id.str();
         const auto block_num = bs->block->block_num();

         auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::microseconds{fc::time_point::now().time_since_epoch().count()});

         auto source =
            "ctx._source.validated = params.validated;"
            "ctx._source.irreversible = params.irreversible;"
            "ctx._source.updateAt = params.updateAt;";

         fc::mutable_variant_object params_doc;
         fc::mutable_variant_object script_doc;

         params_doc("validated", bs->validated);
         params_doc("irreversible", true);
         params_doc("updateAt", now.count());

         script_doc("source", source);
         script_doc("lang", "painless");
         script_doc("params", params_doc);

         if( store_block_states ) {
            fc::mutable_variant_object doc;
            fc::mutable_variant_object block_state_doc;

            block_state_doc("block_num", static_cast<int32_t>(block_num));
            block_state_doc("block_id", block_id_str);
            block_state_doc("block_header_state", bs);
            block_state_doc("validated", bs->validated);
            block_state_doc("irreversible", true);
            block_state_doc("createAt", now.count());

            doc("script", script_doc);
            doc("upsert", block_state_doc);

            fc::mutable_variant_object action_doc;
            action_doc("_index", block_states_index);
            action_doc("_type", "_doc");
            action_doc("_id", block_id_str);
            action_doc("retry_on_conflict", 100);

            auto action = fc::json::to_string( fc::variant_object("update", action_doc) );
            auto json = fc::json::to_string( doc );

            bulker& bulk = bulk_pool->get();
            bulk.append_document(std::move(action), std::move(json));
         }

         if( store_blocks ) {
            fc::mutable_variant_object doc;
            fc::mutable_variant_object block_doc;

            block_doc("block_num", static_cast<int32_t>(block_num));
            block_doc("block_id", block_id_str);
            block_doc("block", abi_deserializer->to_variant_with_abi( *bs->block ));
            block_doc("irreversible", true);
            block_doc("validated", bs->validated);
            block_doc("createAt", now.count());

            doc("script", script_doc);
            doc("upsert", block_doc);


            fc::mutable_variant_object action_doc;
            action_doc("_index", blocks_index);
            action_doc("_type", "_doc");
            action_doc("_id", block_id_str);
            action_doc("retry_on_conflict", 100);

            auto action = fc::json::to_string( fc::variant_object("update", action_doc) );
            auto json = fc::prune_invalid_utf8( fc::json::to_string( doc ) );

            bulker& bulk = bulk_pool->get();
            bulk.append_document(std::move(action), std::move(json));
         }

         if( store_transactions ) {

            for( const auto& receipt : bs->block->transactions ) {
               string trx_id_str;
               if( receipt.trx.contains<packed_transaction>() ) {
                  const auto& pt = receipt.trx.get<packed_transaction>();
                  // get id via get_raw_transaction() as packed_transaction.id() mutates internal transaction state
                  const auto& raw = pt.get_raw_transaction();
                  const auto& trx = fc::raw::unpack<transaction>( raw );
                  if( !filter_include( trx ) ) continue;
                  const auto& id = trx.id();
                  trx_id_str = id.str();
               } else {
                  const auto& id = receipt.trx.get<transaction_id_type>();
                  trx_id_str = id.str();
               }

               fc::mutable_variant_object trans_doc;
               fc::mutable_variant_object doc;

               trans_doc("irreversible", true);
               trans_doc("block_id", block_id_str);
               trans_doc("block_num", static_cast<int32_t>(block_num));
               trans_doc("updateAt", now.count());

               doc("doc", trans_doc);
               doc("doc_as_upsert", true);

               fc::mutable_variant_object action_doc;
               action_doc("_index", trans_index);
               action_doc("_type", "_doc");
               action_doc("_id", trx_id_str);
               action_doc("retry_on_conflict", 100);

               auto action = fc::json::to_string( fc::variant_object("update", action_doc) );
               auto json = fc::json::to_string( doc );

               bulker& bulk = bulk_pool->get();
               bulk.append_document(std::move(action), std::move(json));
            }
         }
      }
   );
}

void elasticsearch_plugin_impl::check_task_queue_size() {
   auto task_queue_size = thread_pool->queue_size();
   if ( task_queue_size > max_task_queue_size ) {
      task_queue_sleep_time += 10;
      if( task_queue_sleep_time > 1000 )
         wlog("thread pool task queue size: ${q}", ("q", task_queue_size));
      boost::this_thread::sleep_for( boost::chrono::milliseconds( task_queue_sleep_time ));
   } else {
      task_queue_sleep_time -= 10;
      if( task_queue_sleep_time < 0 ) task_queue_sleep_time = 0;
   }
}

void elasticsearch_plugin_impl::consume_blocks() {
   try {
      while (true) {
         boost::mutex::scoped_lock lock(mtx);
         while ( transaction_metadata_queue.empty() &&
                 transaction_trace_queue.empty() &&
                 block_state_queue.empty() &&
                 irreversible_block_state_queue.empty() &&
                 !done ) {
            condition.wait(lock);
         }

         // capture for processing
         size_t transaction_metadata_size = transaction_metadata_queue.size();
         if (transaction_metadata_size > 0) {
            transaction_metadata_process_queue = move(transaction_metadata_queue);
            transaction_metadata_queue.clear();
         }
         size_t transaction_trace_size = transaction_trace_queue.size();
         if (transaction_trace_size > 0) {
            transaction_trace_process_queue = move(transaction_trace_queue);
            transaction_trace_queue.clear();
         }
         size_t block_state_size = block_state_queue.size();
         if (block_state_size > 0) {
            block_state_process_queue = move(block_state_queue);
            block_state_queue.clear();
         }
         size_t irreversible_block_size = irreversible_block_state_queue.size();
         if (irreversible_block_size > 0) {
            irreversible_block_state_process_queue = move(irreversible_block_state_queue);
            irreversible_block_state_queue.clear();
         }

         lock.unlock();

         if (done) {
            ilog("draining queue, size: ${q}", ("q", transaction_metadata_size + transaction_trace_size + block_state_size + irreversible_block_size));
         }

         // process transactions
         auto start_time = fc::time_point::now();
         auto size = transaction_trace_process_queue.size();
         while (!transaction_trace_process_queue.empty()) {
            const auto& t = transaction_trace_process_queue.front();
            process_applied_transaction(t);
            transaction_trace_process_queue.pop_front();
         }
         auto time = fc::time_point::now() - start_time;
         auto per = size > 0 ? time.count()/size : 0;
         if( time > fc::seconds(5) ) // reduce logging, 5 secs
            ilog( "process_applied_transaction,  time per: ${p}, size: ${s}, time: ${t}", ("s", size)("t", time)("p", per) );

         start_time = fc::time_point::now();
         size = transaction_metadata_process_queue.size();
         while (!transaction_metadata_process_queue.empty()) {
            const auto& t = transaction_metadata_process_queue.front();
            process_accepted_transaction(t);
            transaction_metadata_process_queue.pop_front();
         }
         time = fc::time_point::now() - start_time;
         per = size > 0 ? time.count()/size : 0;
         if( time > fc::seconds(5) ) // reduce logging, 5 secs
            ilog( "process_accepted_transaction, time per: ${p}, size: ${s}, time: ${t}", ("s", size)( "t", time )( "p", per ));

         // process blocks
         start_time = fc::time_point::now();
         size = block_state_process_queue.size();
         while (!block_state_process_queue.empty()) {
            const auto& bs = block_state_process_queue.front();
            process_accepted_block( bs );
            block_state_process_queue.pop_front();
         }
         time = fc::time_point::now() - start_time;
         per = size > 0 ? time.count()/size : 0;
         if( time > fc::seconds(5) ) // reduce logging, 5 secs
            ilog( "process_accepted_block,       time per: ${p}, size: ${s}, time: ${t}", ("s", size)("t", time)("p", per) );

         // process irreversible blocks
         start_time = fc::time_point::now();
         size = irreversible_block_state_process_queue.size();
         while (!irreversible_block_state_process_queue.empty()) {
            const auto& bs = irreversible_block_state_process_queue.front();
            process_irreversible_block(bs);
            irreversible_block_state_process_queue.pop_front();
         }
         time = fc::time_point::now() - start_time;
         per = size > 0 ? time.count()/size : 0;
         if( time > fc::seconds(5) ) // reduce logging, 5 secs
            ilog( "process_irreversible_block,   time per: ${p}, size: ${s}, time: ${t}", ("s", size)("t", time)("p", per) );

         if( transaction_metadata_size == 0 &&
             transaction_trace_size == 0 &&
             block_state_size == 0 &&
             irreversible_block_size == 0 &&
             done ) {
            break;
         }
      }
      ilog("elasticsearch_plugin consume thread shutdown gracefully");
   } catch (fc::exception& e) {
      elog("FC Exception while consuming block ${e}", ("e", e.to_string()));
   } catch (std::exception& e) {
      elog("STD Exception while consuming block ${e}", ("e", e.what()));
   } catch (...) {
      elog("Unknown exception while consuming block");
   }
}


void elasticsearch_plugin_impl::delete_index() {
   ilog("drop elasticsearch index");
   es_client->delete_index( accounts_index );
   es_client->delete_index( blocks_index );
   es_client->delete_index( trans_index );
   es_client->delete_index( block_states_index );
   es_client->delete_index( trans_traces_index );
   es_client->delete_index( action_traces_index );
}

void elasticsearch_plugin_impl::init() {
   ilog("create elasticsearch index");
   es_client->init_index( accounts_index, accounts_mapping );
   es_client->init_index( blocks_index, blocks_mapping );
   es_client->init_index( trans_index, trans_mapping );
   es_client->init_index( block_states_index, block_states_mapping );
   es_client->init_index( trans_traces_index, trans_traces_mapping );
   es_client->init_index( action_traces_index, action_traces_mapping );

   if (es_client->count_doc(accounts_index) == 0) {
      auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::microseconds{fc::time_point::now().time_since_epoch().count()});
      
      fc::mutable_variant_object account_doc;
      auto acc_name = chain::config::system_account_name;
      account_doc("name", name( acc_name ).to_string());
      account_doc("createAt", now.count());
      account_doc("pub_keys", fc::variants());
      account_doc("account_controls", fc::variants());
      auto json = fc::json::to_string(account_doc);
      try {
         es_client->create(accounts_index, json, std::to_string(acc_name));
      } catch( ... ) {
         handle_elasticsearch_exception( "create system account " + json, __LINE__ );
      }
   }

   ilog("starting elasticsearch plugin thread");
   consume_thread = boost::thread([this] { consume_blocks(); });

   startup = false;
}

elasticsearch_plugin::elasticsearch_plugin():my(new elasticsearch_plugin_impl()){}
elasticsearch_plugin::~elasticsearch_plugin(){}

void elasticsearch_plugin::set_program_options(options_description&, options_description& cfg) {
   cfg.add_options()
         ("elastic-queue-size,q", bpo::value<uint32_t>()->default_value(1024),
         "The target queue size between nodeos and elasticsearch plugin thread.")
         ("elastic-abi-cache-size", bpo::value<uint32_t>()->default_value(2048),
          "The maximum size of the abi cache for serializing data.")
         ("elastic-thread-pool-size", bpo::value<size_t>()->default_value(4),
          "The size of the data processing thread pool.")
         ("elastic-bulker-pool-size", bpo::value<size_t>()->default_value(2),
          "The size of the elasticsearch bulker pool.")
         ("elastic-bulk-size", bpo::value<size_t>()->default_value(5),
          "The size(megabytes) of the each bulk request.")
         ("elastic-index-wipe", bpo::bool_switch()->default_value(false),
         "Required with --replay-blockchain, --hard-replay-blockchain, or --delete-all-blocks to delete elasticsearch index."
         "This option required to prevent accidental wipe of index.")
         ("elastic-block-start", bpo::value<uint32_t>()->default_value(0),
         "If specified then only abi data pushed to elasticsearch until specified block is reached.")
         ("elastic-url,u", bpo::value<std::string>(),
         "elasticsearch URL connection string If not specified then plugin is disabled.")
         ("elastic-user", bpo::value<std::string>()->default_value(""),
         "elasticsearch user.")
         ("elastic-password", bpo::value<std::string>()->default_value(""),
         "elasticsearch password.")
         ("elastic-store-blocks", bpo::value<bool>()->default_value(true),
          "Enables storing blocks in elasticsearch.")
         ("elastic-store-block-states", bpo::value<bool>()->default_value(true),
          "Enables storing block state in elasticsearch.")
         ("elastic-store-transactions", bpo::value<bool>()->default_value(true),
          "Enables storing transactions in elasticsearch.")
         ("elastic-store-transaction-traces", bpo::value<bool>()->default_value(true),
          "Enables storing transaction traces in elasticsearch.")
         ("elastic-store-action-traces", bpo::value<bool>()->default_value(true),
          "Enables storing action traces in elasticsearch.")
         ("elastic-filter-on", bpo::value<vector<string>>()->composing(),
          "Track actions which match receiver:action:actor. Receiver, Action, & Actor may be blank to include all. i.e. eosio:: or :transfer:  Use * or leave unspecified to include all.")
         ("elastic-filter-out", bpo::value<vector<string>>()->composing(),
          "Do not track actions which match receiver:action:actor. Receiver, Action, & Actor may be blank to exclude all.")
         ;
}

void elasticsearch_plugin::plugin_initialize(const variables_map& options) {
   try {
      if( options.count( "elastic-url" )) {
         ilog( "initializing elasticsearch_plugin" );
         my->configured = true;

         if( options.at( "replay-blockchain" ).as<bool>() || options.at( "hard-replay-blockchain" ).as<bool>() || options.at( "delete-all-blocks" ).as<bool>() ) {
            if( options.at( "elastic-index-wipe" ).as<bool>()) {
               ilog( "Wiping elascticsearch index on startup" );
               my->delete_index_on_startup = true;
            } else if( options.count( "elastic-block-start" ) == 0 ) {
               EOS_ASSERT( false, chain::plugin_config_exception, "--elastic-index-wipe required with --replay-blockchain, --hard-replay-blockchain, or --delete-all-blocks"
                                 " --elastic-index-wipe will remove EOS index from elasticsearch." );
            }
         }

         if( options.count( "abi-serializer-max-time-ms") == 0 ) {
            EOS_ASSERT(false, chain::plugin_config_exception, "--abi-serializer-max-time-ms required as default value not appropriate for parsing full blocks");
         }
         my->abi_serializer_max_time = app().get_plugin<chain_plugin>().get_abi_serializer_max_time();

         if( options.count( "elastic-queue-size" )) {
            my->max_queue_size = options.at( "elastic-queue-size" ).as<uint32_t>();
         }
         if( options.count( "elastic-abi-cache-size" )) {
            my->abi_cache_size = options.at( "elastic-abi-cache-size" ).as<uint32_t>();
            EOS_ASSERT( my->abi_cache_size > 0, chain::plugin_config_exception, "elastic-abi-cache-size > 0 required" );
         }
         if( options.count( "elastic-block-start" )) {
            my->start_block_num = options.at( "elastic-block-start" ).as<uint32_t>();
         }
         if( options.count( "elastic-store-blocks" )) {
            my->store_blocks = options.at( "elastic-store-blocks" ).as<bool>();
         }
         if( options.count( "elastic-store-block-states" )) {
            my->store_block_states = options.at( "elastic-store-block-states" ).as<bool>();
         }
         if( options.count( "elastic-store-transactions" )) {
            my->store_transactions = options.at( "elastic-store-transactions" ).as<bool>();
         }
         if( options.count( "elastic-store-transaction-traces" )) {
            my->store_transaction_traces = options.at( "elastic-store-transaction-traces" ).as<bool>();
         }
         if( options.count( "elastic-store-action-traces" )) {
            my->store_action_traces = options.at( "elastic-store-action-traces" ).as<bool>();
         }
        if( options.count( "elastic-filter-on" )) {
            auto fo = options.at( "elastic-filter-on" ).as<vector<string>>();
            my->filter_on_star = false;
            for( auto& s : fo ) {
               if( s == "*" ) {
                  my->filter_on_star = true;
                  break;
               }
               std::vector<std::string> v;
               boost::split( v, s, boost::is_any_of( ":" ));
               EOS_ASSERT( v.size() == 3, fc::invalid_arg_exception, "Invalid value ${s} for --elastic-filter-on", ("s", s));
               filter_entry fe{v[0], v[1], v[2]};
               my->filter_on.insert( fe );
            }
         } else {
            my->filter_on_star = true;
         }
         if( options.count( "elastic-filter-out" )) {
            auto fo = options.at( "elastic-filter-out" ).as<vector<string>>();
            for( auto& s : fo ) {
               std::vector<std::string> v;
               boost::split( v, s, boost::is_any_of( ":" ));
               EOS_ASSERT( v.size() == 3, fc::invalid_arg_exception, "Invalid value ${s} for --elastic-filter-out", ("s", s));
               filter_entry fe{v[0], v[1], v[2]};
               my->filter_out.insert( fe );
            }
         }

         if( my->start_block_num == 0 ) {
            my->start_block_reached = true;
         }

         std::string url_str = options.at( "elastic-url" ).as<std::string>();
         if ( url_str.back() != '/' ) url_str.push_back('/');
         std::string user_str = options.at( "elastic-user" ).as<std::string>();
         std::string password_str = options.at( "elastic-password" ).as<std::string>();
         size_t thr_pool_size = options.at( "elastic-thread-pool-size" ).as<size_t>();
         size_t bulk_pool_size = options.at( "elastic-bulker-pool-size" ).as<size_t>();
         size_t bulk_size = options.at( "elastic-bulk-size" ).as<size_t>();

         my->es_client.reset( new elastic_client(std::vector<std::string>({url_str}), user_str, password_str) );
         my->abi_deserializer.reset( new deserializer(
               my->abi_cache_size, my->abi_serializer_max_time, std::vector<std::string>({url_str}), user_str, password_str) );

         ilog("init thread pool, size: ${tps}", ("tps", thr_pool_size));
         my->thread_pool.reset( new ThreadPool(thr_pool_size) );
         my->max_task_queue_size = my->max_queue_size;

         ilog("init bulker pool, size: ${bps}, bulk size: ${bs}mb", ("bps", bulk_pool_size)("bs", bulk_size));
         my->bulk_pool.reset( new bulker_pool(bulk_pool_size, bulk_size * 1024 * 1024, std::vector<std::string>({url_str}), user_str, password_str) );

         // hook up to signals on controller
         chain_plugin* chain_plug = app().find_plugin<chain_plugin>();
         EOS_ASSERT( chain_plug, chain::missing_chain_plugin_exception, ""  );
         auto& chain = chain_plug->chain();
         my->chain_id.emplace( chain.get_chain_id());

         my->accepted_block_connection.emplace(
            chain.accepted_block.connect( [&]( const chain::block_state_ptr& bs ) {
            my->accepted_block( bs );
         } ));
         my->irreversible_block_connection.emplace(
            chain.irreversible_block.connect( [&]( const chain::block_state_ptr& bs ) {
               my->applied_irreversible_block( bs );
            } ));
         my->accepted_transaction_connection.emplace(
            chain.accepted_transaction.connect( [&]( const chain::transaction_metadata_ptr& t ) {
               my->accepted_transaction( t );
            } ));
         my->applied_transaction_connection.emplace(
            chain.applied_transaction.connect( [&]( const chain::transaction_trace_ptr& t ) {
               my->applied_transaction( t );
            } ));
         if( my->delete_index_on_startup ) {
            my->delete_index();
         }
         my->init();
      } else {
         wlog( "eosio::elasticsearch_plugin configured, but no --elastic-url specified." );
         wlog( "elasticsearch_plugin disabled." );
      }
   }
   FC_LOG_AND_RETHROW()
}

void elasticsearch_plugin::plugin_startup() {
   // Make the magic happen
}

void elasticsearch_plugin::plugin_shutdown() {
   my->accepted_block_connection.reset();
   my->irreversible_block_connection.reset();
   my->accepted_transaction_connection.reset();
   my->applied_transaction_connection.reset();

   my.reset();
}

}
