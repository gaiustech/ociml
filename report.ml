open Printf
  
(** Generate a tabular report in the same format as the Python equivalent 
@param header An array of column headings *)
class report (header : string array) =
  object (self)
    (** Array holding the width of each column *)
    val mutable widths = ([|  |] : int array)
      
    (** List of arrays each of which is one row in the report *)
    val mutable rows = ([] : (string array) list)
      
    (** Initialize widths to be the same as the widths of the header *)
    initializer
      (widths <- Array.make (Array.length header) 0; self#set_widths header)
      
    method private set_widths =
      fun x ->
        for i = 0 to (min (Array.length x) (Array.length header)) - 1 do
          if widths.(i) < (String.length x.(i))
          then widths.(i) <- String.length x.(i)
          else ()
        done
      
    (** Return r.(i) padded with spaces to widths.(i) *)
    method private pad_column = fun r i -> sprintf "%-*s" widths.(i) r.(i)
      
    method private print_row =
      fun chan r ->
        (for i = 0 to (min (Array.length r) (Array.length header)) - 1 do
           output_string chan ((self#pad_column r i) ^ " ")
         done;
         output_string chan "\n")
      
    (** Add a row to this report - column widths will automatically adjust @param r an array of string values*)
    method add_row = fun r -> (self#set_widths r; rows <- r :: rows)
      
    (** Generate the report to STDOUT @param chan an optional out_channel *)
    method print_report =
      fun ?(chan = Pervasives.stdout) () ->
        (self#print_row chan header;
         self#print_row chan
           (Array.init (Array.length widths)
              (fun i -> String.make widths.(i) '-'));
         List.iter (fun x -> self#print_row chan x) (List.rev rows))
      
  end
  
(* End of file *)
