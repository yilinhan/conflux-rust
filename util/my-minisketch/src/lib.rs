use minisketch_rs::Minisketch;

//Wrapper class for minisketch_rs

pub type SketchElementSize = u64;

pub struct MyMinisketch {

}

impl MyMinisketch {
    pub const ELEMENT_SIZE_IN_BITS:u32 =  (std::mem::size_of::<SketchElementSize>() * 8)  as u32;
    pub const CAPACITY:usize= 100;
    pub const IMPLEMENTATION:u32 =0;
    pub const BUCKET_NUM:usize =1;

    pub fn create_empty_minisketch() ->Minisketch{
        let minisketch = Minisketch::try_new(MyMinisketch::ELEMENT_SIZE_IN_BITS, MyMinisketch::IMPLEMENTATION, MyMinisketch::CAPACITY).unwrap();
        minisketch
    }

    pub fn create_empty_minisketches()-> Vec<Minisketch>{
        let mut sketches:Vec<Minisketch> = Vec::new();
        for _i in 0..MyMinisketch::BUCKET_NUM {
            sketches.push(MyMinisketch::create_empty_minisketch());
        }
        sketches
    }

    pub fn add_to_sketch(sketch:&mut Minisketch,v: SketchElementSize){
        sketch.add(v);
    }

    pub fn add_to_sketches( sketches:& mut Vec<Minisketch>, v: Vec<SketchElementSize>){
        for item in v{
            MyMinisketch::add_to_sketch(& mut sketches[item as usize % MyMinisketch::BUCKET_NUM], item);
        }
    }

    pub fn serialize_sketch( sketch: &Minisketch) -> Vec<u8> {
        let mut buf = vec![0u8; sketch.serialized_size()];
        sketch.serialize(&mut buf).expect("Minisketch serialize");
        buf
    }

    pub fn serialize_sketches( sketches: &Vec<Minisketch>) ->Vec<Vec<u8>>{
        let mut buf:Vec<Vec<u8>> = Vec::new();
        for item in sketches{
            buf.push(MyMinisketch::serialize_sketch(item));
        }
        buf
    }

    pub fn deserialize_sketch( serialized_sketch: &Vec<u8>) -> Minisketch {
        let mut minisketch = MyMinisketch::create_empty_minisketch();
        minisketch.deserialize(&serialized_sketch);
        minisketch
    }

    #[deprecated(since="0.1", note="need to implement a serializeb message struct")]
    pub fn deserialize_sketches(serialized_sketches:&Vec<Vec<u8>>)->Vec<Minisketch> {
        let mut minisketches:Vec<Minisketch> = Vec::new();
        for item in serialized_sketches {
            minisketches.push(MyMinisketch::deserialize_sketch(item));
        }
        minisketches
    }

/*
    fn reconcile_serialized_sketch(&self, v: &Vec<u64>, sketch_other: &[u8]) -> Result<Vec<u64>, ()> {
        let mut sketch_local = self.create_minisketch(v);
        let sketch_remote = self.deserialize_sketch(sketch_other);

        sketch_local.merge(&sketch_remote).expect("Minisketch merge");

        let mut diffs = vec![0u64; self.capacity];
        let num_diffs = sketch_local.decode(&mut diffs).map_err(|_| ())?;

        Ok(diffs.into_iter().take(num_diffs).collect())
    }
*/
    pub fn reconcile(local_sketch:&mut Minisketch, remote_sketch:& Minisketch)-> Result<Vec<u64>, ()>{
        local_sketch.merge(remote_sketch).expect("Minisketch merge");

        let mut diffs = vec![0u64; MyMinisketch::CAPACITY];
        let num_diffs = local_sketch.decode(&mut diffs).map_err(|_| ())?;

        Ok(diffs.into_iter().take(num_diffs).collect())
    }

    pub fn reconcile_sketches( buckets_num:usize, local_sketches:&mut Vec<Minisketch>, remote_sketches:&Vec<Minisketch>)->Vec<Result<Vec<u64>,()>> {
        let mut results:Vec<Result<Vec<u64>,()>> = Vec::new();
        for i in 0..buckets_num{
            results.push(MyMinisketch::reconcile(&mut local_sketches[i ],&remote_sketches[i ]));
        }
        results
    }


}