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

#define DEBUG 1

/* retrieve all the column names from the last query done on this statement handles as a string array */
value caml_oci_get_column_names(value handles, value stmt) {
  CAMLparam2(handles, stmt);
  oci_handles_t h = Oci_handles_val(handles);
  OCIStmt* sth = Oci_statement_val(stmt);
  CAMLlocal1(cols);
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
    ub4 col_name_len;

    OCIParamGet(sth, OCI_HTYPE_STMT, h.err, (dvoid**)&ph, i);
    OCIAttrGet((dvoid*)ph, OCI_DTYPE_PARAM, (dvoid**)&col_name, (ub4*)&col_name_len, OCI_ATTR_NAME, h.err);
    col_name[col_name_len] = '\0';

#ifdef DEBUG
    char dbuf[256]; snprintf(dbuf, 255, "caml_oci_get_column_names i=%d len=%d name=%s", i, col_name_len, (char*)col_name); debug(dbuf);
#endif
    Store_field(cols, i - 1, caml_copy_string((char*)col_name));
    OCIDescriptorFree(ph, OCI_DTYPE_PARAM);
  }

  CAMLreturn(cols);
}
/* end of file */
