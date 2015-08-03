/* functions for converting between OCI and OCaml datatypes, needed by bind and fetch */

#include <caml/mlvalues.h>
#include <caml/memory.h>
#include <caml/alloc.h>
#include <caml/custom.h>
#include <caml/callback.h>
#include <caml/fail.h>
#include <string.h>
#include <stdio.h>
#include <time.h>
#include <oci.h>
#include "oci_wrapper.h"

/* convert an epoch time to an Oracle date */
void epoch_to_ocidate(double e, OCIDate* ocidate) {
  time_t t = (time_t)e;
  struct tm ut;
  localtime_r(&t, &ut); /* convert to a Unix time */
  mktime(&ut);

#ifdef DEBUG
  char dbuf[256]; snprintf(dbuf, 255, "epoch_to_ocidate: epoch=%f year=%d month=%d day=%d", e, ut.tm_year + 1900, ut.tm_mon + 1, ut.tm_mday); debug(dbuf);
#endif

  OCIDateSetDate(ocidate, ut.tm_year + 1900, ut.tm_mon + 1, ut.tm_mday); 
  OCIDateSetTime(ocidate, ut.tm_hour, ut.tm_min, ut.tm_sec);
}

/* convert an Oracle date to epoch */
double ocidate_to_epoch(OCIDate* ocidate) {
  int year, month, day, hour, minute, second;
  struct tm ut;

  OCIDateGetDate(ocidate, &year, &month, &day);
  OCIDateGetTime(ocidate, &hour, &minute, &second);

  ut.tm_year = year - 1900; ut.tm_mon = month - 1; ut.tm_mday = day;
  ut.tm_hour = hour;    ut.tm_min = minute;    ut.tm_sec = second;  ut.tm_isdst = -1;

  mktime(&ut); // should fix tm_isdst
  double d = (double)mktime(&ut);

#ifdef DEBUG
  char dbuf[256]; snprintf(dbuf, 255, "ocidate_to_epoch: epoch=%f year=%d month=%d day=%d is_dst=%d", d, ut.tm_year + 1900, ut.tm_mon + 1, ut.tm_mday, ut.tm_isdst); debug(dbuf);
#endif
  return d;
}

/* dereference a pointer to a string */
value caml_oci_get_defined_string(value defs) {
  CAMLparam1(defs);
  oci_define_t d = Oci_defhandle_val(defs);
  
#ifdef DEBUG
  char dbuf[256]; snprintf(dbuf, 255, "caml_oci_get_defined_string: string=%s indicator=%d", (char*)d.ptr, d.ind); debug(dbuf);
#endif

  CAMLreturn(caml_copy_string(d.ptr));
}

/* dereference and return a datetime as epoch */
value caml_oci_get_date_as_double(value defs) {
  CAMLparam1(defs);
  oci_define_t d = Oci_defhandle_val(defs);

  CAMLreturn(caml_copy_double(ocidate_to_epoch(d.ptr)));
}

value caml_oci_get_double(value handles, value defs) {
  CAMLparam2(handles, defs);
  oci_define_t d = Oci_defhandle_val(defs);
  oci_handles_t h = Oci_handles_val(handles);

  double r;

  sword x = OCINumberToReal(h.err, d.ptr, sizeof(double), &r);
  CHECK_OCI(x, h);

  CAMLreturn(caml_copy_double(r));
}

value caml_oci_get_int(value handles, value defs) {
  CAMLparam2(handles, defs);
  oci_define_t d = Oci_defhandle_val(defs);
  oci_handles_t h = Oci_handles_val(handles);

  int r;

  sword x = OCINumberToInt(h.err, d.ptr, sizeof(int), OCI_NUMBER_SIGNED, &r);
  CHECK_OCI(x, h);

#ifdef DEBUG
  char dbuf[256]; snprintf(dbuf, 255, "caml_oci_get_int: returning %d", r); debug(dbuf);
#endif

  CAMLreturn(Val_int(r));
}

/* end of file */
