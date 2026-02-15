#ifndef OPENFGDB_C_API_H
#define OPENFGDB_C_API_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define OFGDB_OK 0
#define OFGDB_ERR_INVALID_ARG 1
#define OFGDB_ERR_NOT_FOUND 2
#define OFGDB_ERR_INTERNAL 3
#define OFGDB_ERR_ALREADY_EXISTS 4

int ofgdb_open(const char *path, uint64_t *db_handle);
int ofgdb_create(const char *path, uint64_t *db_handle);
int ofgdb_close(uint64_t db_handle);

int ofgdb_exec_sql(uint64_t db_handle, const char *sql);

int ofgdb_open_table(uint64_t db_handle, const char *table_name, uint64_t *table_handle);
int ofgdb_close_table(uint64_t db_handle, uint64_t table_handle);

int ofgdb_search(uint64_t table_handle, const char *fields, const char *where_clause, uint64_t *cursor_handle);
int ofgdb_fetch_row(uint64_t cursor_handle, uint64_t *row_handle);
int ofgdb_close_cursor(uint64_t cursor_handle);

int ofgdb_create_row(uint64_t table_handle, uint64_t *row_handle);
int ofgdb_insert(uint64_t table_handle, uint64_t row_handle);
int ofgdb_update(uint64_t table_handle, uint64_t row_handle);
int ofgdb_close_row(uint64_t row_handle);

int ofgdb_get_field_info(uint64_t table_handle, uint64_t *field_info_handle);
int ofgdb_close_field_info(uint64_t field_info_handle);
int ofgdb_field_info_count(uint64_t field_info_handle, int32_t *out_count);
int ofgdb_field_info_name(uint64_t field_info_handle, int32_t index, char **out_name);

int ofgdb_set_string(uint64_t row_handle, const char *column_name, const char *value);
int ofgdb_set_int32(uint64_t row_handle, const char *column_name, int32_t value);
int ofgdb_set_double(uint64_t row_handle, const char *column_name, double value);
int ofgdb_set_blob(uint64_t row_handle, const char *column_name, const uint8_t *data, int32_t size);
int ofgdb_set_geometry(uint64_t row_handle, const uint8_t *wkb, int32_t size);
int ofgdb_set_null(uint64_t row_handle, const char *column_name);

int ofgdb_list_domains(uint64_t db_handle, uint64_t *cursor_handle);
int ofgdb_create_coded_domain(uint64_t db_handle, const char *domain_name, const char *field_type);
int ofgdb_add_coded_value(uint64_t db_handle, const char *domain_name, const char *code, const char *label);
int ofgdb_assign_domain_to_field(uint64_t db_handle, const char *table_name, const char *column_name, const char *domain_name);

int ofgdb_list_relationships(uint64_t db_handle, uint64_t *cursor_handle);
int ofgdb_create_relationship_class(
    uint64_t db_handle,
    const char *name,
    const char *origin_table,
    const char *destination_table,
    const char *origin_pk,
    const char *origin_fk,
    const char *forward_label,
    const char *backward_label,
    const char *cardinality,
    int32_t is_composite,
    int32_t is_attributed
);

/* Convenience string APIs for Java FFM wrappers. The returned heap strings
 * are owned by the caller and must be freed with ofgdb_free_string().
 */
int ofgdb_list_domains_text(uint64_t db_handle, char **out_text);
int ofgdb_list_relationships_text(uint64_t db_handle, char **out_text);
int ofgdb_list_tables_text(uint64_t db_handle, char **out_text);
int ofgdb_list_runtime_info_text(char **out_text);
int ofgdb_row_get_string(uint64_t row_handle, const char *column_name, char **out_value);
int ofgdb_row_is_null(uint64_t row_handle, const char *column_name, int32_t *out_is_null);
int ofgdb_row_get_int32(uint64_t row_handle, const char *column_name, int32_t *out_value);
int ofgdb_row_get_double(uint64_t row_handle, const char *column_name, double *out_value);
int ofgdb_row_get_blob(uint64_t row_handle, const char *column_name, uint8_t **out_data, int32_t *out_size);
int ofgdb_row_get_geometry(uint64_t row_handle, uint8_t **out_wkb, int32_t *out_size);

const char *ofgdb_last_error_message(void);
void ofgdb_free_string(char *value);

#ifdef __cplusplus
}
#endif

#endif
