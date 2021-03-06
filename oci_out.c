/* functions relating to OUT and RETURNING */

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

/* dummy callback that binds no data */
sb4 cbf_no_data(dvoid *ctxp, OCIBind *bindp, ub4 iter, ub4 index,
                      dvoid **bufpp, ub4 *alenpp, ub1 *piecep, dvoid **indpp) {
  *bufpp = (dvoid *) 0;
  *alenpp = 0;
  *indpp = (dvoid *) 0;
  *piecep = OCI_ONE_PIECE;

  return OCI_CONTINUE;
}

/* callback to stash the returned data */
sb4 cbf_get_numeric_data(dvoid *ctxp, OCIBind *bindp, ub4 iter, ub4 index,
		 dvoid **bufpp, ub4 **alenp, ub1 *piecep,
		 dvoid **indpp, ub2 **rcodepp) {

  /* get a pointer to store the pointer to the memory we alloc, and the error handle */
  cb_context_t* cbct = (cb_context_t*)ctxp;
  
  /* find out how many rows we are expecting */
  if (index == 0) {
    int rows = 0;
    //BREAKPOINT
    OCIAttrGet((dvoid*)bindp, OCI_HTYPE_BIND, (dvoid*)&rows, (ub4*)sizeof(int), OCI_ATTR_ROWS_RETURNED, cbct->err);
    int sz = rows * sizeof(out_number_t);
    void* rs = (void*)malloc(sz);
    memcpy(cbct->cht.ptr, &rs, sizeof(void*)); /* ptr is now a pointer to a pointer */
    
#ifdef DEBUG
    char dbuf[256]; snprintf(dbuf, 255, "cbf_get_data: rows=%d rs=%p cbct->cht.ptr=%p bytes=%d", rows, rs, *(void**)cbct->cht.ptr, sz); debug(dbuf);
#endif
  }
  
  /* storage in the order indicator, rc, bufpp, alenp - assume i can get only pointers back */
  out_number_t* offset = (out_number_t*)((*(void**)cbct->cht.ptr) + (index * sizeof(out_number_t)));
  //BREAKPOINT
  *indpp =   (dvoid*)&offset->indicator;
  *rcodepp = (dvoid*)&offset->rc;
  *bufpp =   (dvoid*)&offset->bufpp;

  offset->alenp = sizeof(OCINumber);
  *alenp =   (dvoid*)&offset->alenp;
  *piecep = OCI_ONE_PIECE;

#ifdef DEBUG
  char dbuf[256]; snprintf(dbuf, 255, "cbf_get_data: iter=%d index=%d bindp=%p offset=%p indpp=%p rcodepp=%p bufpp=%p alenp=%p", iter, index, bindp, offset, *indpp, *rcodepp, *bufpp, *alenp); debug(dbuf);
#endif

#ifdef DEBUG
  debug("leaving cbf_get_data");
#endif
  return OCI_CONTINUE;
}

/* get a integer from the memory populated by the callback function */
int get_int_from_context(oci_handles_t h, cb_context_t* cbct, int index) {
  out_number_t* offset = (out_number_t*)((*(void**)cbct->cht.ptr) + (index * sizeof(out_number_t)));
  int r;
  sword x = OCINumberToInt(h.err, &offset->bufpp, sizeof(int), OCI_NUMBER_SIGNED, &r);
  CHECK_OCI(x, h);
  return r;
}

/* get a floating point number from the memory populated by the callback function */
int get_float_from_context(oci_handles_t h, cb_context_t* cbct, int index) {
  out_number_t* offset = (out_number_t*)((*(void**)cbct->cht.ptr) + (index * sizeof(out_number_t)));
  double r;
  sword x = OCINumberToReal(h.err, &offset->bufpp, sizeof(double), &r);
  CHECK_OCI(x, h);
  return r;
}

value caml_oci_get_int_from_context(value handles, value context, value index) {
  CAMLparam3(handles, context, index);
  oci_handles_t h = Oci_handles_val(handles);
  cb_context_t  c = C_context_val(context);
  int i = Int_val(index);
  //BREAKPOINT
  int r = get_int_from_context(h, &c, i);

  CAMLreturn(Val_int(r));
}

value caml_oci_get_float_from_context(value handles, value context, value index) {
  CAMLparam3(handles, context, index);
  oci_handles_t h = Oci_handles_val(handles);
  cb_context_t  c = C_context_val(context);
  int i = Int_val(index);
  //BREAKPOINT
  double r = get_float_from_context(h, &c, i);

  CAMLreturn(caml_copy_double(r));
}


/* callback function to free memory, called by the OCaml GC */
void caml_free_context_t(value cbct) {
  CAMLparam1(cbct);
#ifdef DEBUG
  debug("caml_free_context_t: entered");
#endif
  cb_context_t x = C_context_val(cbct);
  free(x.cht.ptr); // only free the callback memory, not the error handle!
  CAMLreturn0;
}


static struct custom_operations c_context_t_custom_ops = {
  "c_alloc_t_custom_ops", &caml_free_context_t, NULL, NULL, NULL, NULL}; 


/* allocate a bind handle as (using oci_alloc_bindhandle) ,OCIBindByPos with OCI_DATA_AT_EXEC, 
   then OCIBindDynamic with callback. Context of callback is pointer. Callback
   allocates some memory then sets the pointer to that. Assume for now that 
   OCINumber is the biggest thing

   In ML, the bind handle lives in bound_vals and the callback pointer
   lives in oci_ptrs. 
*/

/* do it for integers */
value caml_oci_bind_numeric_out_by_pos(value handles, value stmt, value bindh, value pos) {
  CAMLparam4(handles, stmt, bindh, pos);
  oci_handles_t h = Oci_handles_val(handles);
  OCIStmt* s = Oci_statement_val(stmt);
  OCIBind* bh = Oci_bindhandle_val(bindh);
  int p = Int_val(pos);
  sword x;
  
  /* this is a trick to get the error handle into the callback function - 
     bloody Oracle and their global variables everywhere in their sample 
     code */
  cb_context_t* cbct = (cb_context_t*)malloc(sizeof(cb_context_t));;
  
  /* allocate a pointer to a pointer */
  cbct->cht.ptr = (void*)malloc(sizeof(void*));
  cbct->err     = h.err;
  //BREAKPOINT

#ifdef DEBUG
  char dbuf[256]; snprintf(dbuf, 255, "caml_oci_bind_numeric_out_by_pos: pos=%d cbct=%p cbct.cht.ptr=%p", p, cbct, cbct->cht.ptr); debug(dbuf);
#endif

  /* bind by position */
  OCINumber on; int z = 0;
  x = OCINumberFromInt(h.err, &z, sizeof(int), OCI_NUMBER_SIGNED, &on);
  CHECK_OCI(x, h);

  x = OCIBindByPos(s, &bh, h.err, (ub4)p, (dvoid*)&on, sizeof(OCINumber), SQLT_VNU, 0, 0, 0, 0, 0, OCI_DATA_AT_EXEC);
  CHECK_OCI(x, h);

  /* bind in the callback */
  x = OCIBindDynamic(bh, h.err, (dvoid*)cbct, cbf_no_data, (dvoid*)cbct, cbf_get_numeric_data);
  CHECK_OCI(x, h);

  value v = caml_alloc_custom(&c_context_t_custom_ops, sizeof(cb_context_t), 0, 1);
  C_context_val(v) = *cbct;
  CAMLreturn(v);
}

/* callback to stash the returned date */
sb4 cbf_get_date(dvoid *ctxp, OCIBind *bindp, ub4 iter, ub4 index,
		 dvoid **bufpp, ub4 **alenp, ub1 *piecep,
		 dvoid **indpp, ub2 **rcodepp) {

  /* get a pointer to store the pointer to the memory we alloc, and the error handle */
  cb_context_t* cbct = (cb_context_t*)ctxp;
  
  /* find out how many rows we are expecting */
  if (index == 0) {
    int rows = 0;
    OCIAttrGet((dvoid*)bindp, OCI_HTYPE_BIND, (dvoid*)&rows, (ub4*)sizeof(int), OCI_ATTR_ROWS_RETURNED, cbct->err);
    int sz = rows * sizeof(out_date_t);
    void* rs = (void*)malloc(sz);
    memcpy(cbct->cht.ptr, &rs, sizeof(void*)); /* ptr is now a pointer to a pointer */
#ifdef DEBUG
    char dbuf[256]; snprintf(dbuf, 255, "cbf_get_date: rows=%d rs=%p cbct->cht.ptr=%p bytes=%d", rows, rs, *(void**)cbct->cht.ptr, sz); debug(dbuf);
#endif
  }
  
  /* storage in the order indicator, rc, bufpp, alenp - assume i can get only pointers back */
  out_date_t* offset = (out_date_t*) ((*(void**)cbct->cht.ptr) + (index * sizeof(out_date_t)));
  //BREAKPOINT
  *indpp =   (dvoid*)&offset->indicator;
  *rcodepp = (dvoid*)&offset->rc;
  *bufpp =   (dvoid*)&offset->bufpp;

  offset->alenp = sizeof(OCIDate);
  *alenp =   (dvoid*)&offset->alenp;
  *piecep = OCI_ONE_PIECE;

#ifdef DEBUG
  char dbuf[256]; snprintf(dbuf, 255, "cbf_get_date: iter=%d index=%d bindp=%p offset=%p indpp=%p rcodepp=%p bufpp=%p alenp=%p", iter, index, bindp, offset, *indpp, *rcodepp, *bufpp, *alenp); debug(dbuf);
#endif

#ifdef DEBUG
  debug("leaving cbf_get_date");
#endif
  return OCI_CONTINUE;
}


/* do it for dates */
value caml_oci_bind_date_out_by_pos(value handles, value stmt, value bindh, value pos) {
  CAMLparam4(handles, stmt, bindh, pos);
  oci_handles_t h = Oci_handles_val(handles);
  OCIStmt* s = Oci_statement_val(stmt);
  OCIBind* bh = Oci_bindhandle_val(bindh);
  int p = Int_val(pos);
  sword x;
  
  cb_context_t* cbct = (cb_context_t*)malloc(sizeof(cb_context_t));;
  cbct->cht.ptr = (void*)malloc(sizeof(void*));
  cbct->err     = h.err;

#ifdef DEBUG
  char dbuf[256]; snprintf(dbuf, 255, "caml_oci_bind_date_out_by_pos: pos=%d cbct=%p cbct.cht.ptr=%p", p, cbct, cbct->cht.ptr); debug(dbuf);
#endif

  /* bind by position */
  OCIDate d;
  double epoch = 0.0;
  epoch_to_ocidate(epoch, &d);
  x = OCIBindByPos(s, &bh, h.err, (ub4)p, (dvoid*)&d, sizeof(OCIDate), SQLT_ODT, 0, 0, 0, 0, 0, OCI_DATA_AT_EXEC);
  CHECK_OCI(x, h);

  /* bind in the callback */
  x = OCIBindDynamic(bh, h.err, (dvoid*)cbct, cbf_no_data, (dvoid*)cbct, cbf_get_date);
  CHECK_OCI(x, h);

  value v = caml_alloc_custom(&c_context_t_custom_ops, sizeof(cb_context_t), 0, 1);
  C_context_val(v) = *cbct;
  CAMLreturn(v);
}

/* get a floating point number from the memory populated by the callback function */
double get_date_from_context(oci_handles_t h, cb_context_t* cbct, int index) {
  out_date_t* offset = (out_date_t*)(*(void**)(cbct->cht.ptr) + (index * sizeof(out_date_t)));
  double ed = ocidate_to_epoch(&offset->bufpp);;
#ifdef DEBUG
  char dbuf[256]; snprintf(dbuf, 255, "get_date_from_context: epoch time is %.0f", ed); debug(dbuf);
#endif

  return ed;
}

value caml_oci_get_date_from_context(value handles, value context, value index) {
  CAMLparam3(handles, context, index);
  oci_handles_t h = Oci_handles_val(handles);
  cb_context_t  c = C_context_val(context);
  int i = Int_val(index);

#ifdef DEBUG
  char dbuf[256]; snprintf(dbuf, 255, "caml_oci_get_date_from_context: entered, index=%d", i); debug(dbuf);
#endif

  double d = get_date_from_context(h, &c, i);
#ifdef DEBUG
  snprintf(dbuf, 255, "caml_oci_get_date_from_context: epoch time is %.0f", d); debug(dbuf);
#endif
  CAMLreturn(caml_copy_double(d));
}

/* same again for strings */
sb4 cbf_get_string(dvoid *ctxp, OCIBind *bindp, ub4 iter, ub4 index,
		 dvoid **bufpp, ub4 **alenp, ub1 *piecep,
		 dvoid **indpp, ub2 **rcodepp) {

  cb_context_t* cbct = (cb_context_t*)ctxp;
  
  /* find out how many rows we are expecting */
  if (index == 0) {
    int rows = 0;
    OCIAttrGet((dvoid*)bindp, OCI_HTYPE_BIND, (dvoid*)&rows, (ub4*)sizeof(int), OCI_ATTR_ROWS_RETURNED, cbct->err);
    int sz = rows * sizeof(out_string_t);
    void* rs = (void*)malloc(sz);
    memcpy(cbct->cht.ptr, &rs, sizeof(void*)); /* ptr is now a pointer to a pointer */
#ifdef DEBUG
    char dbuf[256]; snprintf(dbuf, 255, "cbf_get_string: rows=%d rs=%p cbct->cht.ptr=%p bytes=%d", rows, rs, *(void**)cbct->cht.ptr, sz); debug(dbuf);
#endif
  }
  
  out_string_t* offset = (out_string_t*) ((*(void**)cbct->cht.ptr) + (index * sizeof(out_string_t)));
  *indpp =   (dvoid*)&offset->indicator;
  *rcodepp = (dvoid*)&offset->rc;
  *bufpp =   (dvoid*)&offset->bufpp;

  offset->alenp = MAXVARCHAR;
  *alenp =   (dvoid*)&offset->alenp;
  *piecep = OCI_ONE_PIECE;

#ifdef DEBUG
  char dbuf[256]; snprintf(dbuf, 255, "cbf_get_string: iter=%d index=%d bindp=%p offset=%p indpp=%p rcodepp=%p bufpp=%p alenp=%p", iter, index, bindp, offset, *indpp, *rcodepp, *bufpp, *alenp); debug(dbuf);
#endif

#ifdef DEBUG
  debug("leaving cbf_get_string");
#endif
  return OCI_CONTINUE;
}


/* do it for dates */
value caml_oci_bind_string_out_by_pos(value handles, value stmt, value bindh, value pos) {
  CAMLparam4(handles, stmt, bindh, pos);
  oci_handles_t h = Oci_handles_val(handles);
  OCIStmt* s = Oci_statement_val(stmt);
  OCIBind* bh = Oci_bindhandle_val(bindh);
  int p = Int_val(pos);
  sword x;
  
  cb_context_t* cbct = (cb_context_t*)malloc(sizeof(cb_context_t));
  cbct->cht.ptr = (void*)malloc(sizeof(void*));
  cbct->err     = h.err;

#ifdef DEBUG
  char dbuf[256]; snprintf(dbuf, 255, "caml_oci_bind_string_out_by_pos: pos=%d cbct=%p cbct.cht.ptr=%p", p, cbct, cbct->cht.ptr); debug(dbuf);
#endif

  /* bind by position */
  x = OCIBindByPos(s, &bh, h.err, (ub4)p, (dvoid*)0, MAXVARCHAR, SQLT_CHR, 0, 0, 0, 0, 0, OCI_DATA_AT_EXEC);
  CHECK_OCI(x, h);

  /* bind in the callback */
  x = OCIBindDynamic(bh, h.err, (dvoid*)cbct, cbf_no_data, (dvoid*)cbct, cbf_get_string);
  CHECK_OCI(x, h);

  value v = caml_alloc_custom(&c_context_t_custom_ops, sizeof(cb_context_t), 0, 1);
  C_context_val(v) = *cbct;
  CAMLreturn(v);
}

/* get a string from the memory populated by the callback function */
char* get_string_from_context(oci_handles_t h, cb_context_t* cbct, int index) {
  out_string_t* offset = (out_string_t*)(*(void**)(cbct->cht.ptr) + (index * sizeof(out_string_t)));
  offset->bufpp[offset->alenp] = '\0';

#ifdef DEBUG
  char dbuf[256]; snprintf(dbuf, 255, "get_string_from_context: alenp=%d string='%s'", offset->alenp, offset->bufpp); debug(dbuf);
#endif
  
  return offset->bufpp;
}

value caml_oci_get_string_from_context(value handles, value context, value index) {
  CAMLparam3(handles, context, index);
  oci_handles_t h = Oci_handles_val(handles);
  cb_context_t  c = C_context_val(context);
  int i = Int_val(index);

#ifdef DEBUG
  char dbuf[256]; snprintf(dbuf, 255, "caml_oci_get_string_from_context: entered, index=%d", i); debug(dbuf);
#endif

  char* s = get_string_from_context(h, &c, i);

  CAMLreturn(caml_copy_string(s));
}

/* get the position of a named bind - this is a bit of a mess but 
   OCIStmtGetBindInfo() doesn't do what I want it to do*/
value caml_oci_get_pos_from_name(value handles, value stmt, value bind_name) {
  CAMLparam3(handles, stmt, bind_name);
  oci_handles_t h = Oci_handles_val(handles);
  OCIStmt* s = Oci_statement_val(stmt);
  char* nm = String_val(bind_name);
  int i = 0; int r = -1;

  /* temporary variables - for once I am glad of stack alloc  */
  sb4 found = 0;
  text* bvns[255];
  ub1 bvnls[255];
  text* invs[255];
  ub1 invls[255];
  ub1 dupls[255];
  OCIBind* bhnds[255];
  OCIStmtGetBindInfo(s, h.err, (ub4)255, (ub4)1, &found, bvns, bvnls, invs, invls, dupls, bhnds);

#ifdef DEBUG
  char dbuf[256]; snprintf(dbuf, 255, "caml_oci_get_pos_from_name: looking for %s", nm); debug(dbuf);
#endif

  for (i = 0; i < found; i++) {
#ifdef DEBUG
    snprintf(dbuf, 255, "caml_oci_get_pos_from_name: found name %s at pos %d", (char*)bvns[i], i + 1); debug(dbuf);
#endif
    if ((strncmp(nm, (char*)bvns[i], bvnls[i])) == 0) {
      r = i + 1;
    }
  }

  CAMLreturn(Val_int(r));
}

/* a "/dev/null" buffer for dumping return codes I don't care about right now 
   into, e.g. alen of a ref cursor is meaningless */
ub4 dummy = 0;

/* callback for ref cursors - we get the statement as context, then point the buffer at it */
sb4 cbf_ref_cursor(dvoid *ctxp, OCIBind *bindp, ub4 iter, ub4 index,
		 dvoid **bufpp, ub4 **alenp, ub1 *piecep,
		 dvoid **indpp, ub2 **rcodepp) {

  OCIStmt **stmtp = (OCIStmt **) ctxp;

#ifdef DEBUG
  char dbuf[256]; snprintf(dbuf, 255, "cbf_ref_cursor: *bufpp=%p", stmtp); debug(dbuf);
#endif

  *bufpp = stmtp;
  *alenp = (void*)&dummy;
  *piecep = OCI_ONE_PIECE;
  *indpp = (void*)&dummy;
  *rcodepp = (void*)&dummy;

  return OCI_CONTINUE;
}

sb4 cbf_ref_cursor_in(dvoid *ctxp, OCIBind *bindp, ub4 iter, ub4 index,
                      dvoid **bufpp, ub4 *alenpp, ub1 *piecep, dvoid **indpp) {
  OCIStmt **stmtp = (OCIStmt **) ctxp;

#ifdef DEBUG
  char dbuf[256]; snprintf(dbuf, 255, "cbf_ref_cursor_in: *bufpp=%p", stmtp); debug(dbuf);
#endif

  *bufpp = stmtp;
  *alenpp = dummy;
  *indpp = (void*)&dummy;
  *piecep = OCI_ONE_PIECE;

  return OCI_CONTINUE;
}

value caml_oci_bind_ref_cursor(value handles, value statement, value bindh, value pos, value refcursor) {
  CAMLparam5(handles, statement, bindh, pos, refcursor);
  oci_handles_t h = Oci_handles_val(handles);
  OCIStmt* s = Oci_statement_val(statement);
  OCIBind* bh = Oci_bindhandle_val(bindh);
  int p = Int_val(pos);
  OCIStmt* rc = Oci_statement_val(refcursor);
  sword x;

#ifdef DEBUG
  char dbuf[256]; snprintf(dbuf, 255, "caml_oci_bind_ref_cursor: rc=%p", rc); debug(dbuf);
#endif

  x = OCIBindByPos(s, &bh, h.err, (ub4)p, NULL, 0, SQLT_RSET, 0, 0, 0, 0, 0, OCI_DATA_AT_EXEC);
  CHECK_OCI(x, h);

  x = OCIBindDynamic(bh, h.err, (dvoid*)rc, cbf_ref_cursor_in, (dvoid*)rc, cbf_ref_cursor);
  CHECK_OCI(x, h);
  
  CAMLreturn(Val_unit);
}

/* end of file */
