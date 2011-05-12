/* function related to SELECT and fetch */

#include <caml/mlvalues.h>
#include <caml/memory.h>
#include <caml/alloc.h>
#include <caml/custom.h>
#include <caml/callback.h>
#include <caml/fail.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <oci.h>
#include <ocidfn.h>
#include "oci_wrapper.h"

#define DEBUG

/* Retrieve all the column names from the last query done on this statement handles as a string array */
value caml_oci_get_column_types(value handles, value stmt) {
  CAMLparam2(handles, stmt);
  oci_handles_t h = Oci_handles_val(handles);
  OCIStmt* sth = Oci_statement_val(stmt);
  CAMLlocal2(cols, coltuple);
  int i;
  sword x;

  /* get the number of columns in the query */
  int numcols = 0;
  x = OCIAttrGet(sth, OCI_HTYPE_STMT, &numcols, 0, OCI_ATTR_PARAM_COUNT, h.err);
  if (x != OCI_SUCCESS) {
    oci_non_success(h);
  }
  /* allocate some OCaml storage for this number */
  cols = caml_alloc(numcols, 0);

  /* iterate through the set, storing each one - Oracle counts from 1, OCaml and C count from 0 */
  for (i = 1; i <= numcols; i++) {
    OCIParam * ph;
    text* col_name;
    int col_name_len;
    int col_type = 0;
    int col_size = 0;
    int col_scale = 0; /* 0 is false */
    int col_null = 1;

    OCIParamGet(sth, OCI_HTYPE_STMT, h.err, (dvoid**)&ph, i);
    OCIAttrGet((dvoid*)ph, OCI_DTYPE_PARAM, (dvoid**)&col_name, (ub4*)&col_name_len, OCI_ATTR_NAME, h.err);
    col_name[col_name_len] = '\0';

    OCIAttrGet((dvoid*)ph, OCI_DTYPE_PARAM, (dvoid**)&col_type, (ub4*)0, OCI_ATTR_DATA_TYPE, h.err);
    OCIAttrGet((dvoid*)ph, OCI_DTYPE_PARAM, (dvoid**)&col_scale, (ub4*)0, OCI_ATTR_SCALE, h.err); /* to differentiate float from int in fetch */
    OCIAttrGet((dvoid*)ph, OCI_DTYPE_PARAM, (dvoid**)&col_size, (ub4*)0, OCI_ATTR_DATA_SIZE, h.err); 
    OCIAttrGet((dvoid*)ph, OCI_DTYPE_PARAM, (dvoid**)&col_null, (ub4*)0, OCI_ATTR_IS_NULL, h.err);

#ifdef DEBUG
    char dbuf[256]; snprintf(dbuf, 255, "caml_oci_get_column_names i=%d len=%d name=%s type=%d size=%d scale=%d", i, col_name_len, (char*)col_name, col_type, col_size, col_scale); debug(dbuf);
#endif
    coltuple = caml_alloc_tuple(5);
    Store_field(coltuple, 0, caml_copy_string((char*)col_name));  /* name */
    Store_field(coltuple, 1, Val_long(col_type));                 /* type */    
    Store_field(coltuple, 2, Val_long(col_size));                 /* size (for VARCHAR) */

    if (col_scale == 0 && col_type == 2 ) {                       /* is_integer */
      Store_field(coltuple, 3, Val_bool(1));               
    } else {
      Store_field(coltuple, 3, Val_bool(0));               
    }

    Store_field(coltuple, 4, Val_bool(col_null));                /* is_nullable */
    
    Store_field(cols, i - 1, coltuple);
    OCIDescriptorFree(ph, OCI_DTYPE_PARAM);
  }

  CAMLreturn(cols);
}

/* end of file */
