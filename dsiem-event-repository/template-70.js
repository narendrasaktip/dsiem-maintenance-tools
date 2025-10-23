###############################################################################
# Dsiem {siem_plugin_type} Plugin
# Type: SID
# New Generate by Naren
###############################################################################

# The filters below assumes that we'll run on a dedicated pipeline for dsiem event normalization.
# this means that parsed events should be coming to this pipeline from an input definition such as:
#
# input { pipeline { address => dsiemEvents } }
#
# and that the identifying field [fields][log_type] should have already been set to
# "{log_type}" by the previous pipeline.

filter {

# 1st step: identify the source log based on previously parsed field and value.
# 2nd step: mark it as normalizedEvent using [@metadata]

    mutate {
      id => "tag normalizedEvent {plugin_id}"
      add_field => {
        "[@metadata][siem_plugin_type]" => "{siem_plugin_type}"
        "[@metadata][siem_data_type]" => "normalizedEvent"
      }
    }
  }
}

# 3rd step: the actual event normalization so that it matches the format that dsiem expect.
#
# Required fields:
#   timestamp (date), title (string), sensor (string), product (string), dst_ip (string), src_ip (string)
#
# For PluginRule type plugin, the following are also required:
#   plugin_id (integer), plugin_sid (integer)
#
# For TaxonomyRule type plugin, the following is also required:
#   category (string)
#
# Optional fields:
# These fields are optional but should be included whenever possible since they can be used in directive rules:
#   dst_port (integer), src_port (integer), protocol (string), subcategory (string)
#
# These fields are also optional and can be used in directive rules. They should be used for custom data that
# are not defined in standard SIEM fields.
#   custom_label1 (string), custom_data1 (string), custom_label2 (string), custom_data2 (string)
#   custom_label3 (string), custom_data3 (string)
#
# And this field is optional, and should be included if the original logs are also stored in elasticsearch.
# This will allow direct pivoting from alarm view in the web UI to the source index.
#   src_index_pattern (string)
#
# As for other fields from source log, they will be removed by logstash plugin prune below

filter {
  if [@metadata][siem_plugin_type] == "{log_type}" {
    translate {
      id => "plugin_sid lookup {plugin_id}"
      field => "[{field}]"
      destination => "[plugin_sid]"
      dictionary_path => "{dictionary_path}"
      refresh_interval => {refresh_interval}
      fallback => "_translate_failed"
    }

    if [plugin_sid] == "_translate_failed" {
      drop {}
    }

    date {
      id => "timestamp {plugin_id}"
      match => [ "[{timestamp}]", "ISO8601" ]
      target => [timestamp]
    }
    mutate {
      id => "siem_event fields {plugin_id}"
      replace => {
        "title" => "%{[{field}]}"
        "src_index_pattern" => "{src_index_pattern}"
        "sensor" => "%{[{sensor}]}"
        "product" => "%{[{product}]}"
        "src_ip" => "%{[{src_ips}]}"
        "dst_ip" => "%{[{dst_ips}]}"
        "protocol" => "%{[{protocol}]}"
        "category" => "{category}"

        "plugin_id" => "{plugin_id}"
        "src_port" => "%{[{src_port}]}"
        "dst_port" => "%{[{dst_port}]}"
        "custom_label1" => "{custom_label1}"
        "custom_label2" => "{custom_label2}"
        "custom_label3" => "{custom_label3}"
        "custom_data1" => "%{[{custom_data1}]}"
        "custom_data2" => "%{[{custom_data2}]}"
        "custom_data3" => "%{[{custom_data3}]}"
      }
    }

    mutate {
      id => "integer fields {plugin_id}"
      convert => {
        "plugin_id" => "integer"
        "plugin_sid" => "integer"
        "src_port" => "integer"
        "dst_port" => "integer"
      }
    }

    if [custom_data1] == "%{[{custom_data1}]}" { mutate { remove_field => [ "custom_label1", "custom_data1" ]}}
    if [custom_data2] == "%{[{custom_data2}]}" { mutate { remove_field => [ "custom_label2", "custom_data2" ]}}
    if [custom_data3] == "%{[{custom_data3}]}" { mutate { remove_field => [ "custom_label3", "custom_data3" ]}}

    # delete fields except those included in the whitelist below
    prune {
      whitelist_names => [ "@timestamp$" , "^timestamp$", "@metadata", "^src_index_pattern$", "^title$", "^sensor$", "^product$",
        "^src_ip$", "^dst_ip$", "^plugin_id$", "^plugin_sid$", "^category$", "^subcategory$",
        "^src_port$", "^dst_port$", "^protocol$", "^custom_label1$", "^custom_label2$", "^custom_label3$",
        "^custom_data1$", "^custom_data2$", "^custom_data3$" ]
    }
  }
}