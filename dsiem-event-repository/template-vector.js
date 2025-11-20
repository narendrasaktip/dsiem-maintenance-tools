###############################################################################
# Dsiem {siem_plugin_type} Vector Plugin
# Type: SID
# New Generate by Naren
###############################################################################

enrichment_tables:
  enrichment-table_plugin-sid_{siem_plugin_type}:
    type: "file"
    file:
      path: "/etc/dsiem-plugin-tsv/{siem_plugin_type}_plugin-sids.tsv"
      encoding:
        type: "csv"
        delimiter: "\t"
    schema:
      plugin: "string"
      id: "integer"
      sid: "integer"
      title: "string"

transforms:

  # this filter only allows {siem_plugin_type} to pass through, all other events like dns query etc
  # will be dropped

  filter_dsiem-plugin_{siem_plugin_type}:
    type: filter
    inputs:
      - 98_output_to_dsiem_{log_type}.siem_events
    condition:
      type: "vrl"
      source: |-
        {filter}

  transform_dsiem-plugin_{siem_plugin_type}:
    type: remap
    inputs:
      - filter_dsiem-plugin_{siem_plugin_type}
    drop_on_abort: true
    drop_on_error: true
    
    # to automatically remove unused fields, first we set the fields we want to keep
    # in the .norm_event object. Then later on when all fields are collected, we set
    # the root object to .norm_event, which will remove all other unused fields.
    # this is a better method than using del().
    
    # the most important thing below is the lookup of plugin_id and plugin_sid from the
    # enrichment table. This is done by calling get_enrichment_table_record() function.
    # If the record is not found, the transform will abort and the event will be dropped.
    # notice several fields below are hard-coded because the demo log generator doesn't
    # supply them. In actual case we would get them from the log source.

    source: |-
      norm_event.sensor = {sensor}
      norm_event.timestamp = {timestamp}
      norm_event.@timestamp = .@timestamp
      norm_event.src_ip = {src_ips}
      norm_event.dst_ip = {dst_ips}
      norm_event.src_port = {src_port}
      norm_event.dst_port = {dst_port} 
      norm_event.protocol = {protocol}
      norm_event.product = {product}
      norm_event.category = {category}
      norm_event.subcategory = {subcategory}
      row, err = get_enrichment_table_record("enrichment-table_plugin-sid_{siem_plugin_type}", { "title": .{field_name} })
      if err != null {
        abort
      }
      norm_event.plugin_id = row.id
      norm_event.plugin_sid = row.sid
      norm_event.custom_label1 = {custom_label1}
      norm_event.custom_data1 = {custom_data1}
      norm_event.custom_label2 = {custom_label2}
      norm_event.custom_data2 = {custom_data2}
      norm_event.custom_label3 = {custom_label3}
      norm_event.custom_data3 = {custom_data3}
      norm_event.title = row.title
      . = norm_event
      .index_name = "siem_events"
      .event_id = uuid_v4()