
-define(prefix_to_back_sup_name(Prefix), list_to_atom(atom_to_list(Prefix) ++ "_back_sup")).
-define(prefix_to_back_proxy_sup_name(Prefix), list_to_atom(atom_to_list(Prefix) ++ "_back_proxy_sup")).
-define(prefix_to_back_listener_name(Prefix), list_to_atom(atom_to_list(Prefix) ++ "_back_listener")).