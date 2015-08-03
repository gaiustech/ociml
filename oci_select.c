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
  CHECK_OCI(x, h);
  /* allocate some OCaml storage for this number */
  cols = caml_alloc(numcols, 0);

  /* iterate through the set, storing each one - Oracle counts from 1, OCaml and C count from 0 */
  for (i = 1; i <= numcols; i++) {
    OCIParam * ph;
    text* col_name = NULL;
    int col_name_len = 0;
    int col_type = 0;
    int col_size = 0;
    int col_scale = 0; /* 0 is false */
    int col_null = 1;

    OCIParamGet(sth, OCI_HTYPE_STMT, h.err, (dvoid**)&ph, i);
    x = OCIAttrGet((dvoid*)ph, OCI_DTYPE_PARAM, (dvoid**)&col_name, (ub4*)&col_name_len, OCI_ATTR_NAME, h.err);
    CHECK_OCI(x, h);

    char* col_name2 = (char*)malloc(col_name_len+1);
    strncpy(col_name2, (char*)col_name, col_name_len);
    col_name2[col_name_len] ='\0';

    OCIAttrGet((dvoid*)ph, OCI_DTYPE_PARAM, (dvoid**)&col_type, (ub4*)0, OCI_ATTR_DATA_TYPE, h.err);
    OCIAttrGet((dvoid*)ph, OCI_DTYPE_PARAM, (dvoid**)&col_scale, (ub4*)0, OCI_ATTR_SCALE, h.err); /* to differentiate float from int in fetch */
    OCIAttrGet((dvoid*)ph, OCI_DTYPE_PARAM, (dvoid**)&col_size, (ub4*)0, OCI_ATTR_DATA_SIZE, h.err); 
    OCIAttrGet((dvoid*)ph, OCI_DTYPE_PARAM, (dvoid**)&col_null, (ub4*)0, OCI_ATTR_IS_NULL, h.err);

#ifdef DEBUG
    char dbuf[256]; snprintf(dbuf, 255, "caml_oci_get_column_types i=%d len=%d name=%s type=%d size=%d scale=%d null=%d", i, col_name_len, (char*)col_name2, col_type, col_size, col_scale, col_null); debug(dbuf);
#endif
    coltuple = caml_alloc_tuple(5);
    Store_field(coltuple, 0, caml_copy_string((char*)col_name2));  /* name */
    Store_field(coltuple, 1, Val_long(col_type));                 /* type */    
    Store_field(coltuple, 2, Val_long(col_size + 1));                 /* size (for VARCHAR) */ /* +1 fixes problem with SELECT 1 FROM DUAL actually returned as 1. - we undo this cosmetically in oradesc */

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

/* Called by the OCaml garbage collector when a oci_defhandle_t structure is freed - 
   define handle is automatically freed by OCI when the statement handle is freed */
void caml_oci_free_defhandle(value dh) {
  CAMLparam1(dh);
  oci_define_t x = Oci_defhandle_val(dh);
  free(x.ptr);
  CAMLreturn0;
}

static struct custom_operations oci_defhandle_custom_ops = {"oci_defhandle_custom_ops", &caml_oci_free_defhandle, NULL, NULL, NULL, NULL};

/* allocate sufficient memory to store a particular column then return a pointer to it */
value caml_oci_define(value handles, value stmt, value pos, value dtype, value size) {
  CAMLparam5(handles, stmt, pos, dtype, size);
  CAMLlocal1(r);
  r = caml_alloc_tuple(4);
  oci_handles_t h = Oci_handles_val(handles);
  OCIStmt* sth = Oci_statement_val(stmt);
  int p = Int_val(pos); /* C and OCaml count from 0, Oracle counts from 1 */
  
  int t = Int_val(Field(dtype, 0)); /* data type */
  int ii = Int_val(Field(dtype, 1)); /* is_int */

  int s = Int_val(size);

  oci_define_t defs = { NULL, NULL, 0, 0.0, 0 };
  defs.dtype = dtype;

  sword x = -1;
  
#ifdef DEBUG
  char dbuf[256]; snprintf(dbuf, 255, "caml_oci_define: defining for pos=%d dtype=%d is_int=%d size=%d", p + 1, t, ii, s); debug(dbuf);
#endif

  switch (t) {
  case SQLT_CHR:
    defs.ptr = (void*)malloc(s+1);
    memset(defs.ptr, 0, s+1); /* fixes problem with odd behavior of SELECT NULL FROM DUAL */
    x = OCIDefineByPos(sth, &defs.defh, h.err, p + 1, defs.ptr, s+1, SQLT_STR, &defs.ind, 0, 0, OCI_DEFAULT);
    break;
  case SQLT_DAT: /* see if we can get this as an OCIDate... */
    defs.ptr = (void*)malloc(sizeof(OCIDate));
    x = OCIDefineByPos(sth, &defs.defh, h.err, p + 1, defs.ptr, s+1, SQLT_ODT, &defs.ind, 0, 0, OCI_DEFAULT);
    break;
  case SQLT_NUM:
    defs.ptr = (void*)malloc(sizeof(OCINumber));
      x = OCIDefineByPos(sth, &defs.defh, h.err, p + 1, defs.ptr, s, SQLT_VNU, &defs.ind, 0, 0, OCI_DEFAULT);
      break;
  default:
    debug("caml_oci_define: unknown datatype to define");
  }
  
  CHECK_OCI(x, h);
  
  value v = caml_alloc_custom(&oci_defhandle_custom_ops, sizeof(oci_define_t), 0, 1);
  Oci_defhandle_val(v) = defs;
  
  Store_field(r, 0, Val_int(t));
  Store_field(r, 1, Val_bool(ii));
  int is_null = defs.ind < 0;
  Store_field(r, 2, Val_bool(is_null));
  Store_field(r, 3, v);

  CAMLreturn(r);
}

value caml_oci_fetch(value handles, value stmt) {
  CAMLparam2(handles, stmt);
  oci_handles_t h = Oci_handles_val(handles);
  OCIStmt* sth = Oci_statement_val(stmt);

  sword x = OCIStmtFetch2(sth, h.err, 1, OCI_FETCH_NEXT, 1, OCI_DEFAULT);
  CHECK_OCI(x, h);

  CAMLreturn(Val_unit);
}

value caml_oci_set_prefetch(value handles, value stmt, value rows) {
  CAMLparam3(handles, stmt, rows);
  oci_handles_t h = Oci_handles_val(handles);
  OCIStmt* sth = Oci_statement_val(stmt);
  int r = Int_val(rows);

  sword x = OCIAttrSet(sth, OCI_HTYPE_STMT, &r, sizeof(int), OCI_ATTR_PREFETCH_ROWS, h.err);
  CHECK_OCI(x, h);
#ifdef DEBUG
  char dbuf[256]; snprintf(dbuf, 255, "caml_oci_set_prefetch: prefetching (up to) %d rows", r); debug(dbuf);
#endif

  CAMLreturn(Val_unit);
}

value caml_oci_get_rows_affected(value handles, value stmt) {
  CAMLparam2(handles, stmt);
  oci_handles_t h = Oci_handles_val(handles);
  OCIStmt* sth = Oci_statement_val(stmt);
  int r = 0;

  sword x = OCIAttrGet(sth, OCI_HTYPE_STMT, &r, NULL, OCI_ATTR_ROW_COUNT, h.err);
  CHECK_OCI(x, h);

#ifdef DEBUG
  char dbuf[256]; snprintf(dbuf, 255, "caml_oci_get_rows_affected: %d rows", r); debug(dbuf);
#endif

  CAMLreturn(Val_int(r));
}

/* end of file */
