use arc_swap::ArcSwap;
use axum::{
    extract::State,
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use chrono::{DateTime, Utc, NaiveDate};
use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use futures_util::{pin_mut, TryStreamExt};
use lazy_static::lazy_static;
use regex::Regex;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use std::{
    collections::HashMap,
    env,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime},
    vec
};
use tokio_postgres::{types::Oid, NoTls};
use tracing::{debug, error, info, Level};
use tracing_subscriber::FmtSubscriber;
use utoipa::{OpenApi, ToSchema};
use utoipa_swagger_ui::SwaggerUi;

#[derive(OpenApi)]
#[openapi(
    paths(root, tables, hosts, update, read, create, delete),
    components(schemas(
        Host,
        crate::Path,
        Create,
        Read,
        Delete,
        Update,
        Field,
        crate::Data,
        crate::Row,
        crate::Hosts,
        crate::Tables,
        crate::Data
    ))
)]
pub struct ApiDoc;

pub struct Helper {
    tuple_structure: ArcSwap<HashMap<String, Vec<(String, String)>>>,
    server_structure: ArcSwap<Vec<String>>,
    last_refresh: Arc<Mutex<SystemTime>>,
    pools: HashMap<String, Pool>,
    operations: HashMap<String, bool>,
    changes: String,
    changes_path: String,
}

impl Helper {
    pub fn new() -> Self {
        Helper {
            tuple_structure: ArcSwap::new(
                Arc::new(<HashMap<String, Vec<(String, String)>>>::new()),
            ),
            server_structure: ArcSwap::new(Arc::new(Vec::<String>::new())),
            last_refresh: Arc::new(Mutex::new(SystemTime::UNIX_EPOCH)),
            pools: HashMap::new(),
            operations: HashMap::from([
                ("=".to_string(), false),
                ("<".to_string(), false),
                (">".to_string(), false),
                ("<=".to_string(), false),
                (">=".to_string(), false),
                ("ilike".to_string(), true),
            ]),
            changes: "".to_string(),
            changes_path: "".to_string(),
        }
    }

    fn can_refresh(&self) -> bool {
        let hour = Duration::from_secs(3600);
        let mut locked = self.last_refresh.lock().unwrap();
        if locked.elapsed().unwrap() < hour {
            false
        } else {
            *locked = SystemTime::now();
            true
        }
    }

    async fn get_from_server_cache(&self) -> Vec<String> {
        self.refresh().await;
        let mut params: Vec<String> = Vec::new();
        let temp_hash = self.server_structure.load();
        for val in temp_hash.iter() {
            params.push(val.to_owned());
        }
        params
    }

    async fn get_from_cache(&self, value: &String) -> Vec<(String, String)> {
        self.refresh().await;
        let mut params: Vec<(String, String)> = Vec::new();
        let temp_hash = self.tuple_structure.load();
        if temp_hash.contains_key(value) {
            let cached_vec = &temp_hash[value];
            for val in cached_vec {
                params.push(val.to_owned());
            }
        }
        params
    }

    fn add_to_hashmap(
        new_hashmap: &mut HashMap<String, Vec<(String, String)>>,
        together: String,
        left: String,
        right: String,
    ) {
        let vec = new_hashmap.entry(together).or_insert_with(|| Vec::new());
        let combi = (left, right);
        if !vec.contains(&combi) {
            vec.push(combi);
        }
    }

    async fn refresh(&self) {
        if !self.can_refresh() {
            return;
        }
        info!("Refreshing {:?} pools", self.pools.len());
        let mut new_hashmap = HashMap::<String, Vec<(String, String)>>::new();
        Self::add_to_hashmap(&mut new_hashmap, "grpc.trx.credits".to_string(), "".to_string(), "".to_string()); //allow pass trough for credits on grpc
        let mut new_server_vec = Vec::<String>::new();
        for (db, pool) in &self.pools {
            debug!("Structure for {:?}", db);
            let client = pool.get().await.unwrap();
            let params: Vec<String> = vec![];
            let it = client.query_raw(
            "SELECT t.oid, string_agg(e.enumlabel, '|' ORDER BY e.enumsortorder) FROM pg_catalog.pg_type t 
            JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace 
            JOIN pg_catalog.pg_enum e ON t.oid = e.enumtypid 
            GROUP BY 1",
            params
        ).await.unwrap();
            pin_mut!(it);
            let mut map: HashMap<u32, String> = HashMap::new();
            while let Some(row) = it.try_next().await.unwrap() {
                let oid: Oid = row.get(0);
                let options: String = row.get(1);
                map.insert(oid, options);
            }
            let params: Vec<String> = vec![];
            let it = client.query_raw("
            SELECT n.nspname, t.typname, a.attname::text, pg_catalog.format_type ( a.atttypid, a.atttypmod ), a.atttypid, COALESCE(v.viewname ,'') FROM pg_catalog.pg_attribute a 
            JOIN pg_catalog.pg_type t ON a.attrelid = t.typrelid 
            JOIN pg_catalog.pg_namespace n ON ( n.oid = t.typnamespace ) 
            left outer join pg_catalog.pg_views v on n.nspname = v.schemaname and t.typname = v.viewname
            WHERE a.attnum > 0 and n.nspname not in ('pg_catalog', 'information_schema') 
			 AND NOT a.attisdropped order by concat(n.nspname, '.', t.typname) asc",

             /* 
            let it = client.query_raw(
            "SELECT n.nspname, t.typname, a.attname::text, pg_catalog.format_type ( a.atttypid, a.atttypmod ), a.atttypid FROM pg_catalog.pg_attribute a 
            JOIN pg_catalog.pg_type t ON a.attrelid = t.typrelid 
            JOIN pg_catalog.pg_namespace n ON ( n.oid = t.typnamespace ) 
            WHERE a.attnum > 0 and n.nspname not in ('pg_catalog', 'information_schema') AND NOT a.attisdropped order by concat(n.nspname, '.', t.typname) ASC",
            */
            params
        ).await.unwrap();
            pin_mut!(it);
            while let Some(row) = it.try_next().await.unwrap() {
                let schema: String = row.get(0);
                let table: String = row.get(1);
                let column: String = row.get(2);
                let mut cat: String = row.get(3);
                let oid: Oid = row.get(4);
                let view: String = row.get(5);
                if map.contains_key(&oid) {
                    cat = map.get(&oid).unwrap().to_string();
                }
                let readonly;
                if view.len() > 0 {
                    readonly = "readonly.";
                } else {
                    readonly = "";
                }
                if !new_server_vec.contains(db) {
                    new_server_vec.push(db.to_string());
                }
                let together = format!("{}{}.{}.{}", readonly, db, schema, table);
                Self::add_to_hashmap(&mut new_hashmap, db.to_string(), schema, table);
                Self::add_to_hashmap(&mut new_hashmap, together, column, cat);
            }
        }

        self.tuple_structure.store(Arc::new(new_hashmap));
        self.server_structure.store(Arc::new(new_server_vec));
    }

    async fn where_clause(
        &self,
        table: HashMap<String, String>,
        where_clause: Vec<Field>,
    ) -> Option<String> {
        let mut first_field = true;
        let mut together: String = "".to_string();
        for field in &where_clause {
            if !table.contains_key(&field.name) {
                info!("Field {} found but not in table", &field.name);
                return None;
            }
            let clause = self.get_as_string(field);
            if clause.is_none() {
                info!("Field {} is empty", &field.name);
                return None;
            }
            if !first_field {
                together = together + " AND " + &clause.unwrap();
            } else {
                together = together + " WHERE " + &clause.unwrap();
                first_field = false;
            }
        }
        Some(together)
    }

    fn format_to_string(stringvalue: &String, keep: bool) -> Option<String> {
        if STRNULL.eq(stringvalue) {
            return Some(stringvalue.to_string());
        } else if RE.is_match(&stringvalue) || stringvalue.len() < 1 {
            if keep {
                return Some(stringvalue.to_string());
            } else {
                return Some(format!("'{}'", stringvalue.to_string()));
            }
        }
        None
    }

    fn get_as_string(&self, field: &Field) -> Option<String> {
        let rel;
        if field.relation.is_none() {
            rel = "=";
        } else {
            rel = field.relation.as_ref().unwrap();
        }
        if !self.operations.contains_key(rel) {
            info!("Unknown relation {} for field {}", rel, &field.name);
            return None;
        }
        if field.value.eq("null") {
            return Some(format!("{} isnull",field.name));
        }
        let is_ilike = rel.eq("ilike");
        let val = Self::format_to_string(&field.value, is_ilike);
        if val.is_none() {
            return None;
        } else if is_ilike {
            Some(format!(
                "CAST({} AS TEXT) ilike '{}%'",
                field.name,
                val.unwrap()
            ))
        } else {
            Some(format!("{} {} {}", field.name, rel, val.unwrap()))
        }
    }


    async fn change(&self, ops: &String, payload: &String) -> StatusCode {
        if self.changes_path.len() < 3 {
            return StatusCode::NOT_ACCEPTABLE;
        }
        let together = format!(
            "insert into {} (operation, msg) VALUES ('{}','{}');",self.changes_path, ops, payload);
        info!(together);
        let pool = self.pools.get(&self.changes);
        let client = pool.unwrap().get().await.unwrap();
        let params: Vec<String> = vec![];
        let it = client.query_raw(&together, params).await.unwrap();
        pin_mut!(it);
        let res = it.try_next().await;
        match res {
            Ok(_v) => {}
            Err(_e) => {
                return StatusCode::NOT_ACCEPTABLE;
            }
        }
        let rows = it.rows_affected();
        if rows.unwrap() == 1 {
            StatusCode::OK
        } else {
            StatusCode::NOT_ACCEPTABLE
        }
    }
}

lazy_static! {
    static ref RE: Regex = Regex::new(r"^[.a-zA-Z0-9 ()\-:]+$").unwrap();
    static ref STRNULL: String = "NULL".to_string();
}

impl Default for Helper {
    fn default() -> Self {
        Self::new()
    }
}
#[derive(ToSchema, Serialize)]
struct Hosts {
    hosts: Vec<String>,
}
#[derive(ToSchema, Serialize)]
struct Tables {
    tables: Vec<Path>,
}
#[derive(ToSchema, Deserialize, Serialize)]
struct Create {
    who: String,
    review: Option<bool>,
    path: Path,
    data: Vec<Field>,
}
#[derive(ToSchema, Deserialize)]
struct Host {
    name: String,
}

#[derive(Serialize, Deserialize, ToSchema)]
struct Field {
    name: String,
    relation: Option<String>,
    value: String,
}
#[derive(Serialize, Deserialize, ToSchema)]
struct Path {
    server: String,
    schema: String,
    table: String,
}
#[derive(ToSchema, Deserialize, Serialize)]
struct Delete {
    who: String,
    review: Option<bool>,
    path: Path,
    r#where: Vec<Field>,
}
#[derive(ToSchema, Deserialize, Serialize)]
struct Update {
    who: String,
    review: Option<bool>,
    path: Path,
    r#where: Vec<Field>,
    data: Vec<Field>,
}
#[derive(ToSchema, Deserialize)]

struct Read {
    path: Path,
    column: Vec<String>,
    r#where: Vec<Field>,
    page: i32,
    page_size: Option<i32>,
}

#[derive(ToSchema, Serialize)]
struct Data {
    rows: Vec<Row>,
}

#[derive(ToSchema, Serialize)]
struct Row {
    row: Vec<String>,
}

#[utoipa::path(
    get,
    path = "/",
    responses(
        (status = 200, description = "Check service")
    )
)]

async fn root(State(_state): State<Arc<Helper>>) -> &'static str {
    "Alive"
}

#[utoipa::path(
    get,
    path = "/hosts",
    responses(
        (status = 200, description = "Databases", body = Hosts)
    )
)]

async fn hosts(State(state): State<Arc<Helper>>) -> Json<Hosts> {
    let vec = state.get_from_server_cache().await;
    let foundhosts = Hosts { hosts: vec };
    Json(foundhosts)
}

#[utoipa::path(
    post,
    path = "/tables",
    responses(
        (status = 200, description = "Tables", body = Tables)
    )
)]
async fn tables(
    State(state): State<Arc<Helper>>,
    Json(payload): Json<Host>,
) -> (StatusCode, Json<Tables>) {
    let mut tables: Vec<Path> = vec![];
    let vec_tables = state.get_from_cache(&payload.name).await;
    for table in vec_tables {
        tables.push(Path {
            server: payload.name.to_string(),
            schema: table.0,
            table: table.1,
        });
    }
    let tables = Tables { tables };
    (StatusCode::FOUND, Json(tables))
}
#[utoipa::path(
    post,
    path = "/create",
    responses(
        (status = 200, description = "Create")
    )
)]
async fn create(State(state): State<Arc<Helper>>, Json(payload): Json<Create>) -> StatusCode {
    let together = format!(
        "{}.{}.{}",
        payload.path.server, payload.path.schema, payload.path.table
    );
    let vec_columns = state.get_from_cache(&together).await;
    if vec_columns.len() < 1 {
        return StatusCode::NOT_FOUND;
    }

    if payload.review.is_some() && payload.review.unwrap() {
        return state.change(&"create".to_string(), &serde_json::to_string_pretty(&payload).unwrap()).await;
    } else {
        let m: HashMap<_, _> = vec_columns.into_iter().collect();
        let mut together = format!(
            "insert into {}.{} (",
            payload.path.schema, payload.path.table
        );
        let mut first_field = true;
        for field in &payload.data {
            if !m.contains_key(&field.name) {
                info!("Field {} is not in {}", &field.name, payload.path.table);
                return StatusCode::BAD_REQUEST;
            }
            if !first_field {
                together = together + ", " + &field.name;
            } else {
                together = together + &field.name;
                first_field = false;
            }
        }
        together = together + ") VALUES (";
        let mut first_field = true;
        for field in &payload.data {
            let clean = Helper::format_to_string(&field.value, false);
            if clean.is_none() {
                info!("{} has invalid content", &field.name);
                return StatusCode::BAD_REQUEST;
            }
            if !first_field {
                together = together + ", " + clean.unwrap().as_str();
            } else {
                together = together + clean.unwrap().as_str();
                first_field = false;
            }
        }
        together = together + ");";
        //info!(together);
        let pool = state.pools.get(&payload.path.server);
        let client = pool.unwrap().get().await.unwrap();
        let params: Vec<String> = vec![];
        let it = client.query_raw(&together, params).await.unwrap();
        pin_mut!(it);
        let res = it.try_next().await;
        match res {
            Ok(_v) => {}
            Err(_e) => {
                error!("{} failed", &together);
                return StatusCode::NOT_ACCEPTABLE;
            }
        }
        let rows = it.rows_affected();
        if rows.unwrap() == 1 {
            StatusCode::OK
        } else {
            error!("{} failed", &together);
            StatusCode::NOT_ACCEPTABLE
        }
    }
}

#[utoipa::path(
    post,
    path = "/read",
    responses(
        (status = 200, description = "Read", body = Data)
    )
)]
async fn read(
    State(state): State<Arc<Helper>>,
    Json(payload): Json<Read>,
) -> (StatusCode, Json<Data>) {
    let page_size = payload.page_size.unwrap_or(10);
    let mut data: Vec<Row> = Vec::new();
    let mut together = format!(
        "{}.{}.{}",
        payload.path.server, payload.path.schema, payload.path.table
    );
    let mut vec_columns = state.get_from_cache(&together).await;
    if vec_columns.len() < 1 {
        //check if this is a view
        together = format!(
            "readonly.{}.{}.{}",
            payload.path.server, payload.path.schema, payload.path.table
        );
        vec_columns = state.get_from_cache(&together).await;
    }
    if vec_columns.len() < 1 {
        return (StatusCode::NOT_FOUND, Json(Data { rows: data }));
    }
    if payload.column.len() < 1 {
        let mut values: Vec<String> = Vec::new();
        for column in &vec_columns {
            values.push(column.0.to_string());
        }
        data.push(Row { row: values });
        let mut values: Vec<String> = Vec::new();
        for column in &vec_columns {
            values.push(column.1.to_string());
        }
        data.push(Row { row: values });
    } else {
        let m: HashMap<_, _> = vec_columns.into_iter().collect();
        let mut together = format!("select ");
        let mut first_field = true;
        for field in payload.column {
            if !m.contains_key(&field) {
                info!("Field {} is not in {}", &field, payload.path.table);
                return (StatusCode::BAD_REQUEST, Json(Data { rows: data }));
            }
            let field_type = m[&field].to_string();
            let mut field_manip = field.to_string();
            if field_type.contains("|") {
                field_manip = format!("concat({}, '') as {}", field, field);
            }
            if !first_field {
                together = together + ", " + &field_manip;
            } else {
                together = together + &field_manip;
                first_field = false;
            }
        }
        let where_clause = state.where_clause(m, payload.r#where).await;
        if where_clause.is_none() {
            return (StatusCode::NOT_FOUND, Json(Data { rows: data }));
        }
        together = together
            + format!(
                " from {}.{} {} LIMIT {} OFFSET {}",
                payload.path.schema,
                payload.path.table,
                where_clause.unwrap(),
                page_size,
                page_size * payload.page
            )
            .as_str();
        //
        info!("{:?}", together);
        let pool = state.pools.get(&payload.path.server);
        let client = pool.unwrap().get().await.unwrap();
        let params: Vec<String> = vec![];
        let res = client.query_raw(&together, params).await;
        if res.is_err() {
            return (StatusCode::NOT_FOUND, Json(Data { rows: data }));
        }
        let it = res.unwrap();
        pin_mut!(it);
        while let Some(row) = it.try_next().await.unwrap() {
            let mut values: Vec<String> = Vec::new();
            for (col_index, column) in row.columns().iter().enumerate() {
                let coltype = column.type_().to_string();
                if coltype.eq("int8") {
                    let val: Option<i64> = row.get(col_index);
                    if val.is_some() {
                        values.push(val.unwrap().to_string())
                    } else {
                        values.push(STRNULL.to_string());
                    }
                } else if coltype.eq("int2") {
                    let val: Option<i16> = row.get(col_index);
                    if val.is_some() {
                        values.push(val.unwrap().to_string())
                    } else {
                        values.push(STRNULL.to_string());
                    }
                } else if coltype.eq("int4") {
                    let val: Option<i32> = row.get(col_index);
                    if val.is_some() {
                        values.push(val.unwrap().to_string())
                    } else {
                        values.push(STRNULL.to_string());
                    }
                } else if coltype.eq("varchar") || coltype.eq("text") {
                    let val: Option<String> = row.get(col_index);
                    if val.is_some() {
                        values.push(val.unwrap().to_string())
                    } else {
                        values.push(STRNULL.to_string());
                    }
                } else if coltype.eq("bool") {
                    let val: Option<bool> = row.get(col_index);
                    if val.is_some() {
                        if val.unwrap() {
                            values.push("true".to_string());
                        } else {
                            values.push("false".to_string());
                        }
                    }
                } else if coltype.eq("numeric") {
                    let val: Option<Decimal> = row.get(col_index);
                    if val.is_some() {
                        values.push(val.unwrap().to_string())
                    } else {
                        values.push(STRNULL.to_string());
                    }
                } else if coltype.eq("timestamptz") || coltype.eq("timestamp") {
                    let val: Option<SystemTime> = row.get(col_index);
                    if val.is_some() {
                        let datetime: DateTime<Utc> = val.unwrap().into();
                        values.push(format!("{}", datetime.format("%Y-%m-%d %H:%M")));
                    } else {
                        values.push(STRNULL.to_string());
                    }
                } else if coltype.eq("date") {
                    let val: Option<NaiveDate> = row.get(col_index);
                    if val.is_some() {
                        let datetime: NaiveDate = val.unwrap().into();
                        values.push(format!("{}", datetime.format("%Y-%m-%d")));
                    } else {
                        values.push(STRNULL.to_string());
                    }
                } else {
                    values.push(STRNULL.to_string());
                    error!("{}", column.type_());
                }
            }
            data.push(Row { row: values });
        }
    }
    (StatusCode::OK, Json(Data { rows: data }))
}

#[utoipa::path(
    post,
    path = "/update",
    responses(
        (status = 200, description = "Update")
    )
)]
async fn update(State(state): State<Arc<Helper>>, Json(payload): Json<Update>) -> StatusCode {
    let together = format!(
        "{}.{}.{}",
        payload.path.server, payload.path.schema, payload.path.table
    );
    let vec_columns = state.get_from_cache(&together).await;
    if vec_columns.len() < 1 {
        return StatusCode::NOT_FOUND;
    }

    if payload.review.is_some() && payload.review.unwrap() {
        return state.change(&"update".to_string(), &serde_json::to_string_pretty(&payload).unwrap()).await;
    } else {
        let m: HashMap<_, _> = vec_columns.into_iter().collect();
        let mut first_field = true;
        let mut together = format!("update {}.{} SET ", payload.path.schema, payload.path.table);
        for field in payload.data {
            if !m.contains_key(&field.name) {
                info!("Field {} is not in {}", &field.name, payload.path.table);
                return StatusCode::BAD_REQUEST;
            }
            let clean = Helper::format_to_string(&field.value, false);
            if clean.is_none() {
                info!("Field {} has invalid content", &field.name);
                return StatusCode::BAD_REQUEST;
            }
            if !first_field {
                together = together + format!(", {} = {}", field.name, clean.unwrap()).as_str();
            } else {
                together = together + format!("{} = {}", field.name, clean.unwrap()).as_str();
                first_field = false;
            }
        }
        let where_clause = state.where_clause(m, payload.r#where).await;
        if where_clause.is_none() {
            return StatusCode::NOT_FOUND;
        }
        let backup_together = format!(
            "{}.{}.{}_history",
            payload.path.server, payload.path.schema, payload.path.table
        );
        let backup_vec_columns = state.get_from_cache(&backup_together).await;
        if backup_vec_columns.len() > 0 {
            let _backup_together = format!(
                "insert into {}.{}_history (item_id, name, item_group)
select item_id, name, item_group from {}.{} where item_id=2",
                payload.path.schema, payload.path.table, payload.path.schema, payload.path.table
            );
            //vec.iter().map(|x| x.to_string() + ",").collect::<String>()
        }
        together = together + where_clause.unwrap().as_str();
        //error!(together);
        let pool = state.pools.get(&payload.path.server);
        let client = pool.unwrap().get().await.unwrap();
        let params: Vec<String> = vec![];
        let it = client.query_raw(&together, params).await.unwrap();
        pin_mut!(it);
        it.try_next().await.unwrap();
        let rows = it.rows_affected();
        if rows.unwrap() > 0 {
            StatusCode::OK
        } else {
            error!("{} failed", &together);
            StatusCode::NOT_ACCEPTABLE
        }
    }
}

#[utoipa::path(
    post,
    path = "/delete",
    responses(
        (status = 200, description = "Delete")
    )
)]
async fn delete(State(state): State<Arc<Helper>>, Json(payload): Json<Delete>) -> StatusCode {
    let together =  format!(
        "{}.{}.{}",
        payload.path.server, payload.path.schema, payload.path.table
    );
    let vec_columns = state.get_from_cache(&together).await;
    if vec_columns.len() < 1 {
        return StatusCode::NOT_FOUND;
    }
    if payload.review.is_some() && payload.review.unwrap() {
        return state.change(&"delete".to_string(), &serde_json::to_string_pretty(&payload).unwrap()).await;
    } else {
        let m: HashMap<_, _> = vec_columns.into_iter().collect();
        let where_clause = state.where_clause(m, payload.r#where).await;
        if where_clause.is_none() {
            return StatusCode::NOT_FOUND;
        }
        let together = format!(
            "DELETE FROM {}.{} {}",
            payload.path.schema,
            payload.path.table,
            where_clause.unwrap().as_str()
        );
        //error!(together);
        let pool = state.pools.get(&payload.path.server);
        let client = pool.unwrap().get().await.unwrap();
        let params: Vec<String> = vec![];
        let it = client.query_raw(&together, params).await.unwrap();
        pin_mut!(it);
        //let orow = it.try_next().await;
        let rows = it.rows_affected();
        if rows.is_none() {
            return StatusCode::NOT_MODIFIED;
        } else if rows.unwrap() == 1 {
            StatusCode::OK
        } else {
            error!("{} failed", &together);
            StatusCode::NOT_ACCEPTABLE
        }
    }
}

#[tokio::main]
async fn main() -> core::result::Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        error!("Only config file expected");
    }
    let configpath = &args[1];
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::DEBUG)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");
    let data = std::fs::read_to_string(configpath).expect("Unable to read file");
    let json: serde_json::Value = serde_json::from_str(&data).expect("JSON was not well-formatted");
    let mut helper: Helper = Default::default();
    if !json["changes"].is_null() {
        let mut pg_config = tokio_postgres::Config::new();
        let user = json["changes"]["user"]
            .as_str()
            .expect("Required user is not set");
        pg_config.user(user);
        let host = json["changes"]["host"]
            .as_str()
            .expect("Required host is not set");
        pg_config.host(host);
        let server = json["changes"]["server"]
            .as_str()
            .expect("Required server is not set");
        pg_config.dbname(server);
        let password = json["changes"]["password"]
            .as_str()
            .expect("Required password is not set");
        pg_config.password(password);
        let mgr_config = ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        };
        let schema = json["changes"]["schema"]
        .as_str()
        .expect("Required schema is not set");
    let table = json["changes"]["table"]
        .as_str()
        .expect("Required table is not set");
    helper.changes_path = format!("{}.{}", schema, table);
    helper.changes = server.to_string();
    let mgr = Manager::from_config(pg_config, NoTls, mgr_config);
        let pool = Pool::builder(mgr).max_size(16).build().unwrap();
        helper.pools.insert(server.to_string(), pool);
    }

    if let Some(schemas) = json["schemas"].as_array() {
        for schema in schemas {
            let server = schema["server"]
                .as_str()
                .expect("Required server is not set");
            if !helper.pools.contains_key(&server.to_string()) {
                let mut pg_config = tokio_postgres::Config::new();
                let user = schema["user"].as_str().expect("Required user is not set");
                pg_config.user(user);
                let host = schema["host"].as_str().expect("Required host is not set");
                pg_config.host(host);
                pg_config.dbname(server);
                let password = schema["password"]
                    .as_str()
                    .expect("Required password is not set");
                pg_config.password(password);
                let mgr_config = ManagerConfig {
                    recycling_method: RecyclingMethod::Fast,
                };
                let mgr = Manager::from_config(pg_config, NoTls, mgr_config);
                let pool = Pool::builder(mgr).max_size(16).build().unwrap();
                helper.pools.insert(server.to_string(), pool);
            }
        }
    } else {
        error!("Array of schema's expected");
    }

    helper.refresh().await;

    let app = Router::new()
        .route("/", get(root))
        .route("/hosts", get(hosts))
        .route("/tables", post(tables))
        .route("/create", post(create))
        .route("/delete", post(delete))
        .route("/read", post(read))
        .route("/update", post(update))
        .merge(SwaggerUi::new("/swagger-ui"))
        .with_state(Arc::new(helper));
    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
    tracing::debug!("listening on {}", addr);
    let listener = TcpListener::bind(&addr).await?;
    axum::serve(listener, app.into_make_service()).await.unwrap();
    info!("Shutting down");
    Ok(())
}
