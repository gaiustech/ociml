(* various utility functions *)

open Unix

let date_to_double t =
  fst (mktime t)

let decode_col_type x =
  match x with
    |2  (* oci_sqlt_num *)    -> "NUMBER"
    |12 (* oci_sqlt_dat *)    -> "DATE"
    |1  (* oci_sqlt_chr *)    -> "VARCHAR2"
    |_  (* something else! *) -> string_of_int x
	  	  


(* enf of file *)
