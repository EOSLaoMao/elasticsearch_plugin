#include <string>

namespace eosio {

const static std::string accounts_mapping = R"(
{
    "mappings": {
        "_doc": {
            "properties": {
                "name": {
                    "type": "text"
                },
                "pub_keys": {
                    "type": "nested"
                },
                "account_controls": {
                    "type": "nested"
                },
                "abi": {
                    "enabled": false
                },
                "ceateAt": {
                    "type": "date"
                },
                "updateAt": {
                    "type": "date"
                }
            }
        }
    }
}
)";

static const std::string blocks_mapping = R"(
{
    "mappings": {
        "_doc": {
            "properties": {
                "createAt": {
                    "type": "date"
                },
                "updateAt": {
                    "type": "date"
                },
                "block": {
                    "enabled": false
                }
            }
        }
    }
}
)";

static const std::string trans_mapping = R"(
{
    "mappings": {
        "_doc": {
            "properties": {
                "createAt": {
                    "type": "date"
                },
                "updateAt": {
                    "type": "date"
                },
                "actions": {
                    "enabled": false
                }
            }
        }
    }
}
)";


static const std::string block_states_mapping = R"(
{
    "mappings": {
        "_doc": {
            "properties": {
                "createAt": {
                    "type": "date"
                },
                "updateAt": {
                    "type": "date"
                },
                "block_header_state": {
                    "enabled": false
                }
            }
        }
    }
}
)";

static const std::string trans_traces_mapping = R"(
{
    "mappings": {
        "_doc": {
            "properties": {
                "createAt": {
                    "type": "date"
                },
                "updateAt": {
                    "type": "date"
                },
                "action_traces": {
                    "enabled": false
                },
                "failed_dtrx_trace": {
                    "enabled": false
                },
                "except": {
                    "enabled": false
                }
            }
        }
    }
}
)";

static const std::string action_traces_mapping = R"(
{
    "mappings": {
        "_doc": {
            "properties": {
                "createAt": {
                    "type": "date"
                },
                "updateAt": {
                    "type": "date"
                },
                "receipt": {
                    "enabled": false
                },
                "act": {
                    "enabled": false
                }
            }
        }
    }
}
)";

}
