
-record(metastore_version, {
    version,
    mfa
}).

-record(metastore_table, {
    name,
    definition,
    auxiliary
}).

%% #metastore_table.name is the name of table
%% #metastore_table.definition is the same as mnesia table definition, mainly used:
%% record_name
%% attributes
%% ram_copies
%% disc_copies
%% disc_only_copies
%% type
%% #metastore_table.auxiliary is the auxiliary table definition, mainly used:
%% match
%% definition of metastore_table for example:
%% #metastore_table{
%%     name = example_table,
%%     definition = [
%%         {record_name, example_record},
%%         {attributes, record_info(fields, example_record)},
%%         {type, ordered_set},
%%         {disc_copies, [node()]}
%%     ],
%%     auxiliary = [
%%         {match, #example_record{field1 = field1_match(), _ = '_'}}
%%     ]
%% }
%% field1_match() -> '_'.
