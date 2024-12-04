// Copyright (c) 2023-2024 CMU Database Group
//
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

use std::sync::Arc;

use console::Style;
use datafusion::catalog_common::MemoryCatalogProviderList;
use datafusion::error::Result;
use datafusion::execution::context::SessionConfig;
use datafusion::execution::runtime_env::RuntimeConfig;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::SessionContext;
use datafusion_optd_cli::exec::{exec_from_commands, exec_from_commands_collect};
use datafusion_optd_cli::print_format::PrintFormat;
use datafusion_optd_cli::print_options::{MaxRows, PrintOptions};
use mimalloc::MiMalloc;
use optd_datafusion_bridge::{DatafusionCatalog, OptdQueryPlanner};
use optd_datafusion_repr::DatafusionOptimizer;
use rand::{thread_rng, Rng};

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = {
        let session_config = SessionConfig::from_env()?.with_information_schema(true);
        let rn_config = RuntimeConfig::new().build()?;
        let mut state = SessionStateBuilder::new()
            .with_config(session_config.clone())
            .with_runtime_env(Arc::new(rn_config));
        let catalog = Arc::new(MemoryCatalogProviderList::new());
        let mut optimizer: DatafusionOptimizer = DatafusionOptimizer::new_physical(
            Arc::new(DatafusionCatalog::new(catalog.clone())),
            true,
        );
        state = state.with_catalog_list(catalog);
        // clean up optimizer rules so that we can plug in our own optimizer
        state = state.with_optimizer_rules(vec![]);
        state = state.with_physical_optimizer_rules(vec![]);
        // Disable limit
        optimizer.optd_optimizer_mut().prop.partial_explore_iter = None;
        optimizer.optd_optimizer_mut().prop.partial_explore_space = None;
        // use optd-bridge query planner
        state = state.with_query_planner(Arc::new(OptdQueryPlanner::new(optimizer)));
        SessionContext::new_with_state(state.build())
    };
    ctx.refresh_catalogs().await?;

    let perfect_optimizer;
    let ctx_perfect = {
        let session_config = SessionConfig::from_env()?.with_information_schema(true);
        let rn_config = RuntimeConfig::new().build()?;
        let mut state = SessionStateBuilder::new()
            .with_config(session_config.clone())
            .with_runtime_env(Arc::new(rn_config));
        let catalog = Arc::new(MemoryCatalogProviderList::new());
        let mut optimizer: DatafusionOptimizer = DatafusionOptimizer::new_physical(
            Arc::new(DatafusionCatalog::new(catalog.clone())),
            true,
        );
        state = state.with_catalog_list(catalog);
        // clean up optimizer rules so that we can plug in our own optimizer
        state = state.with_optimizer_rules(vec![]);
        state = state.with_physical_optimizer_rules(vec![]);
        // Disable limit
        optimizer.optd_optimizer_mut().prop.partial_explore_iter = None;
        optimizer.optd_optimizer_mut().prop.partial_explore_space = None;
        perfect_optimizer = Arc::new(OptdQueryPlanner::new(optimizer));
        // use optd-bridge query planner
        state = state.with_query_planner(perfect_optimizer.clone());
        SessionContext::new_with_state(state.build())
    };
    ctx.refresh_catalogs().await?;
    ctx_perfect.refresh_catalogs().await?;

    let slient_print_options = PrintOptions {
        format: PrintFormat::Table,
        quiet: true,
        maxrows: MaxRows::Limited(5),
        color: false,
    };

    exec_from_commands(
        &ctx,
        vec![
            "create table t1(t1v1 int, t1v2 int);".to_string(),
            "create table t2(t2v1 int);".to_string(),
            "create table t3(t3v2 int);".to_string(),
        ],
        &slient_print_options,
    )
    .await?;

    exec_from_commands(
        &ctx_perfect,
        vec![
            "create table t1(t1v1 int, t1v2 int);".to_string(),
            "create table t2(t2v1 int);".to_string(),
            "create table t3(t3v2 int);".to_string(),
        ],
        &slient_print_options,
    )
    .await?;

    let mut data_progress = [5; 3];
    let mut iter = 0;

    fn do_insert(table: usize, begin: usize, end: usize, repeat: usize) -> String {
        let table_name = match table {
            0 => "t1",
            1 => "t2",
            2 => "t3",
            _ => unreachable!(),
        };
        let values = (begin..end)
            .collect::<Vec<_>>()
            .into_iter()
            .map(|i| {
                let i = if repeat == 1 {
                    i
                } else {
                    thread_rng().gen_range(begin..end)
                };
                if table == 0 {
                    format!("({}, {})", i, i)
                } else {
                    format!("({})", i)
                }
            })
            .collect::<Vec<_>>()
            .join(", ");
        let statement = format!("insert into {} values {}", table_name, values);
        statement
    }
    let statement = do_insert(0, 0, 5, 1);
    exec_from_commands(&ctx, vec![statement.clone()], &slient_print_options).await?;
    exec_from_commands(&ctx_perfect, vec![statement], &slient_print_options).await?;
    let statement = do_insert(1, 0, 5, 1);
    exec_from_commands(&ctx, vec![statement.clone()], &slient_print_options).await?;
    exec_from_commands(&ctx_perfect, vec![statement], &slient_print_options).await?;
    let statement = do_insert(2, 0, 5, 1);
    exec_from_commands(&ctx, vec![statement.clone()], &slient_print_options).await?;
    exec_from_commands(&ctx_perfect, vec![statement], &slient_print_options).await?;

    fn get_join_order(result: Vec<Vec<String>>) -> String {
        result
            .iter()
            .find(|x| x[0] == "physical_plan after optd-join-order")
            .map(|x| &x[1])
            .unwrap()
            .clone()
    }

    let mut correct = 0;

    let green = Style::new().green();

    loop {
        if iter % 5 == 0 {
            for (table, data_progress_item) in data_progress.iter_mut().enumerate() {
                let progress = rand::thread_rng().gen_range(5..=10) * *data_progress_item / 100;
                let progress = progress.max(5);
                let repeat = rand::thread_rng().gen_range(1..=2);
                let begin = *data_progress_item;
                let end = begin + progress;
                *data_progress_item = end;
                let statement = do_insert(table, begin, end, repeat);
                exec_from_commands(&ctx, vec![statement.clone()], &slient_print_options).await?;
                exec_from_commands(&ctx_perfect, vec![statement], &slient_print_options).await?;
            }
        }
        iter += 1;

        let query = "select * from t1, t2, t3 where t1v1 = t2v1 and t1v2 = t3v2;";
        let result = exec_from_commands_collect(&ctx, vec![format!("explain {}", query)]).await?;
        let join_order: String = get_join_order(result);
        exec_from_commands(&ctx, vec![query.to_string()], &slient_print_options).await?;
        exec_from_commands(&ctx, vec![query.to_string()], &slient_print_options).await?;

        {
            let mut guard = perfect_optimizer.optimizer.lock().unwrap();
            let opt = guard.as_mut().unwrap();
            opt.optd_optimizer_mut().disable_rule(1);
            opt.optd_optimizer_mut().disable_rule(2);
        }

        // derive the best order using the alternative optimizer
        let query00 = "select * from t1;";
        let query01 = "select * from t2;";
        let query02 = "select * from t3;";
        let query03 = "select * from t1, t2 where t1v1 = t2v1;";
        let query04 = "select * from t1, t3 where t1v2 = t3v2;";
        let query1 = "select * from t1, t2, t3 where t1v1 = t2v1 and t1v2 = t3v2;";
        let query2 = "select * from t1, t3, t2 where t1v1 = t2v1 and t1v2 = t3v2;";
        let query3 = "select * from t2, t1, t3 where t1v1 = t2v1 and t1v2 = t3v2;";
        let query4 = "select * from t2, t3, t1 where t1v1 = t2v1 and t1v2 = t3v2;";
        let query5 = "select * from t3, t1, t2 where t1v1 = t2v1 and t1v2 = t3v2;";
        let query6 = "select * from t3, t2, t1 where t1v1 = t2v1 and t1v2 = t3v2;";
        exec_from_commands(
            &ctx_perfect,
            vec![
                query00.to_string(),
                query01.to_string(),
                query02.to_string(),
                query03.to_string(),
                query04.to_string(),
                query1.to_string(),
                query1.to_string(),
                query1.to_string(),
                query2.to_string(),
                query2.to_string(),
                query2.to_string(),
                query3.to_string(),
                query3.to_string(),
                query3.to_string(),
                query4.to_string(),
                query4.to_string(),
                query4.to_string(),
                query5.to_string(),
                query5.to_string(),
                query5.to_string(),
                query6.to_string(),
                query6.to_string(),
                query6.to_string(),
            ],
            &slient_print_options,
        )
        .await?;

        {
            let mut guard = perfect_optimizer.optimizer.lock().unwrap();
            let opt = guard.as_mut().unwrap();
            opt.optd_optimizer_mut().enable_rule(1);
            opt.optd_optimizer_mut().enable_rule(2);
        }

        let result =
            exec_from_commands_collect(&ctx_perfect, vec![format!("explain {}", query)]).await?;
        let best_join_order = get_join_order(result);
        correct += if best_join_order == join_order { 1 } else { 0 };
        let out = format!(
            "Iter {:>3}: {} <-> (best) {}, Accuracy: {}/{}={:.3}",
            iter,
            join_order,
            best_join_order,
            correct,
            iter,
            correct as f64 / iter as f64 * 100.0,
        );
        if best_join_order == join_order {
            println!("{}", green.apply_to(out));
        } else {
            println!("{}", out);
        }
        // exec_from_commands(
        //     &ctx_perfect,
        //     &print_options,
        //     vec![r#"
        //         select
        //             (select count(*) from t1) as t1cnt,
        //             (select count(*) from t2) as t2cnt,
        //             (select count(*) from t3) as t3cnt,
        //             (select count(*) from (select * from t1, t2 where t1v1 = t2v1)) as t1t2cnt,
        //             (select count(*) from (select * from t1, t3 where t1v2 = t3v2)) as t1t3cnt,
        //             (select count(*) from (select * from t1, t2, t3 where t1v1 = t2v1 and t1v2 =
        // t3v2)) as out_cnt;"#         .to_string()],
        // )
        // .await;
    }
}
