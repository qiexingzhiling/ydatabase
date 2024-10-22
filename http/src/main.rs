use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use actix_web::{post, web, App, HttpResponse, HttpServer, Responder, Scope};
use actix_web::web::{scope, Bytes};
use kv_data::db::Engine;
use kv_data::options::Options;

#[post("/put")]
async fn put_handler(
    eng: web::Data<Arc<Engine>>,
    data: web::Json<HashMap<String, String>>,
) -> impl Responder {
    for (key, value) in data.iter() {
        if let Err(_) = eng.put(Bytes::from(key.to_string()), Bytes::from(value.to_string())) {
            return HttpResponse::InternalServerError().body("failed to put value in engine");
        }
    }

    HttpResponse::Ok().body("OK")
}

#[actix_web::main]
async fn main()->std::io::Result<()> {
    let mut opts = Options::default();
    opts.dir_path = PathBuf::from("/tmp/bitcask-rs-http");
    let engine = Arc::new(Engine::open(opts).unwrap());

    // 启动 http 服务
    HttpServer::new(move || {
        App::new().app_data(web::Data::new(engine.clone())).service(
            Scope::new("/bitcask")
                .service(put_handler)
                .service(get_handler)
                .service(delete_handler)
                .service(listkeys_handler)
                .service(stat_handler),
        )
    })
        .bind(("127.0.0.1", 8080))?
        .run()
        .await
}
