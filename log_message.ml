(** Functions related to logging and parsing a logfile *)
open Unix
open Printf
  
let days = [| "Sun"; "Mon"; "Tue"; "Wed"; "Thu"; "Fri"; "Sat" |]
let months = [| "Jan"; "Feb"; "Mar"; "Apr"; "May"; "Jun"; "Jul"; "Aug"; "Sep"; "Oct"; "Nov"; "Dec" |]
  
(** List all the indexes of element el in array arr *)
let indexesOf el arr =
  let stk = Stack.create ()
  in
    (Array.iteri (fun i x -> if x = el then Stack.push i stk else ()) arr;
     stk)
  
(** Return the first index of an element in an array e.g. indexOf "b" ["a"; "b"; "c"|] returns 1 *)
let indexOf el arr =
  try Stack.top (indexesOf el arr) with | Stack.Empty -> raise Not_found
  
(** Convert the timestamp applied by the log_message function back into Epoch seconds *)
let epoch_of_log_timestamp ts =
  Scanf.sscanf ts "%3s %3s %2d %2d:%2d:%2d %4d"
    (fun day mon dd hh24 mm ss yyyy ->
       fst (Unix.mktime { Unix.tm_sec = ss; tm_min = mm; tm_hour = hh24;
              tm_mday = dd; tm_mon = indexOf mon months;
              tm_year = yyyy - 1900; tm_wday = indexOf day days; tm_yday = 0; tm_isdst = false; }))
  
(** Log a message to STDOUT in the same format as the Python equivalent *)
let log_message msg =
  let lt = localtime (time ()) in
  let ts =
    sprintf "%3s %3s %2d %02d:%02d:%02d %4d" days.(lt.tm_wday)
      months.(lt.tm_mon) lt.tm_mday lt.tm_hour lt.tm_min lt.tm_sec
      (lt.tm_year + 1900)
  in prerr_endline (ts ^ (": " ^ msg))
  
(* End of file *)
  

