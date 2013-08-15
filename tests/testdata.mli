(* module definition for test data generator *)

type test_status = Pass|Todo|Fail of string|Time of float * float 
val rand_dt : unit -> Ociml.col_value
val rand_dt_list : unit -> Ociml.col_value list
val rand_row : int -> Ociml.col_value list -> Ociml.col_value array
val rand_big_dataset :  Ociml.col_value list -> int -> Ociml.col_value array list
val sublist : int -> int -> 'a list -> 'a list
val dt_list_to_table_sql : string -> Ociml.col_value list -> string
val remove_newline : string -> string
 

(* End of file *)
