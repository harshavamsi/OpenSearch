use jni::JNIEnv;

#[no_mangle]
pub extern "system" fn Java_org_opensearch_search_query_QueryPhase_interceptQuery<'local>(_env: JNIEnv) {
    println!("Hello Rust!");
}