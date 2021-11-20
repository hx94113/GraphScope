#[macro_use]
extern crate lazy_static;
use graph_store::prelude::*;
use pegasus::api::{CorrelatedSubTask, Count, Filter, Limit, Map, Sink};
use pegasus::JobConf;

lazy_static! {
    pub static ref GRAPH: LargeGraphDB<DefaultId, InternalId> = GraphDBConfig::default()
        .root_dir("/Users/robin/datasets/ldbc_1_bin")
        .schema_file("examples/data/ldbc_sample_bin/graph_schema/schema.json")
        .open()
        .expect("Open graph error");
}

fn main() {
    let mut conf = JobConf::new("apply_bug1");
    conf.set_workers(2);
    let src_id = 1 << 56 | 17592186044810;
    let mut result = pegasus::run(conf, move || {
        let index = pegasus::get_current_worker().index;
        move |input, output| {
            let src = if index == 0 {
                input.input_from(
                    (*GRAPH)
                        .get_both_vertices(src_id, Some(&vec![12]))
                        .map(|lv| lv.get_id() as u64),
                )?
            } else {
                input.input_from(vec![].into_iter())?
            };
            src.flat_map(|v| {
                Ok((*GRAPH)
                    .get_in_vertices(v as DefaultId, Some(&vec![0]))
                    .map(|lv| (lv.get_id() as u64, lv.get_label()[0])))
            })?
            .filter(|(_, l)| Ok(*l == 2))?
            .map(|(v, _)| Ok(v))?
            // .limit(1000)?
            .apply(|sub| {
                sub.repartition(|v| Ok(*v))
                    .flat_map(|v| {
                        Ok((*GRAPH)
                            .get_out_vertices(v as DefaultId, None)
                            .map(|v| (v.get_id() as u64, v.get_label()[0])))
                    })?
                    .filter(|(_, l)| Ok(*l == 3))?
                    .map(|(v, _)| Ok(v))?
                    .repartition(|v| Ok(*v))
                    .flat_map(|v| {
                        Ok((*GRAPH)
                            .get_out_vertices(v as DefaultId, None)
                            .map(|v| v.get_id() as u64))
                    })?
                    .limit(1)?
                    .count()
            })?
            .filter_map(|(v, cnt)| if cnt == 0 { Ok(None) } else { Ok(Some(v)) })?
            .count()?
            .sink_into(output)
        }
    })
    .expect("build job failure");

    while let Some(Ok(cnt)) = result.next() {
        // assert_eq!(cnt, 1000);
        println!("{:?}", cnt);
    }
}
