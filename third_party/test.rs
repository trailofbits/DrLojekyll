
#![allow(unused)]
use std::cell::Cell;
use std::time::Instant;

fn main() {
    let mut fuzzer = Fuzzer {
        seed:  Cell::new(0x34cc028e11b4f89c),
        buf:   Vec::new(),
    };
    
    let mut generated = 0usize;
    let it = Instant::now();

    for iters in 1u64.. {
        fuzzer.buf.clear();
        fuzzer.fragment_25(0);
        generated += fuzzer.buf.len();

        // Filter to reduce the amount of times printing occurs
        if (iters & 0xfffff) == 0 {
            let elapsed = (Instant::now() - it).as_secs_f64();
            let bytes_per_sec = generated as f64 / elapsed;
            print!("MiB/sec: {:12.4}\n", bytes_per_sec / 1024. / 1024.);
            println!("{}", String::from_utf8(fuzzer.buf.clone()).unwrap());
        }
    }
}

struct Fuzzer {
    seed:  Cell<usize>,
    buf:   Vec<u8>,
}

impl Fuzzer {
    fn rand(&self) -> usize {
        let mut seed = self.seed.get();
        seed ^= seed << 13;
        seed ^= seed >> 17;
        seed ^= seed << 43;
        self.seed.set(seed);
        seed
    }
    fn fragment_0(&mut self, depth: usize) {
        if depth >= 15 { return; }
        match self.rand() % 2 {
            0 => self.fragment_30(depth + 1),
            1 => self.fragment_43(depth + 1),
            _ => unreachable!(),
        }
    }
    fn fragment_1(&mut self, depth: usize) {
        if depth >= 15 { return; }
        match self.rand() % 2 {
            0 => self.fragment_50(depth + 1),
            1 => self.fragment_52(depth + 1),
            _ => unreachable!(),
        }
    }
    fn fragment_2(&mut self, depth: usize) {
        if depth >= 15 { return; }
        match self.rand() % 15 {
            0 => self.fragment_54(depth + 1),
            1 => self.fragment_56(depth + 1),
            2 => self.fragment_58(depth + 1),
            3 => self.fragment_60(depth + 1),
            4 => self.fragment_62(depth + 1),
            5 => self.fragment_64(depth + 1),
            6 => self.fragment_66(depth + 1),
            7 => self.fragment_68(depth + 1),
            8 => self.fragment_70(depth + 1),
            9 => self.fragment_72(depth + 1),
            10 => self.fragment_74(depth + 1),
            11 => self.fragment_76(depth + 1),
            12 => self.fragment_78(depth + 1),
            13 => self.fragment_80(depth + 1),
            14 => self.fragment_82(depth + 1),
            _ => unreachable!(),
        }
    }
    fn fragment_3(&mut self, depth: usize) {
        if depth >= 15 { return; }
        match self.rand() % 4 {
            0 => self.fragment_84(depth + 1),
            1 => self.fragment_86(depth + 1),
            2 => self.fragment_88(depth + 1),
            3 => self.fragment_90(depth + 1),
            _ => unreachable!(),
        }
    }
    fn fragment_4(&mut self, depth: usize) {
        if depth >= 15 { return; }
        self.fragment_91(depth + 1);
        self.fragment_92(depth + 1);
        self.fragment_93(depth + 1);
        self.fragment_94(depth + 1);
        self.fragment_95(depth + 1);
        self.fragment_96(depth + 1);
        self.fragment_97(depth + 1);
        self.fragment_98(depth + 1);
        self.fragment_99(depth + 1);
        self.fragment_100(depth + 1);
        self.fragment_101(depth + 1);
        self.fragment_102(depth + 1);
        self.fragment_103(depth + 1);
        self.fragment_104(depth + 1);
    }
    fn fragment_5(&mut self, depth: usize) {
        if depth >= 15 { return; }
        match self.rand() % 2 {
            0 => self.fragment_110(depth + 1),
            1 => self.fragment_112(depth + 1),
            _ => unreachable!(),
        }
    }
    fn fragment_6(&mut self, depth: usize) {
        if depth >= 15 { return; }
        match self.rand() % 4 {
            0 => self.fragment_119(depth + 1),
            1 => self.fragment_126(depth + 1),
            2 => self.fragment_133(depth + 1),
            3 => self.fragment_140(depth + 1),
            _ => unreachable!(),
        }
    }
    fn fragment_7(&mut self, depth: usize) {
        if depth >= 15 { return; }
        match self.rand() % 4 {
            0 => self.fragment_145(depth + 1),
            1 => self.fragment_150(depth + 1),
            2 => self.fragment_155(depth + 1),
            3 => self.fragment_164(depth + 1),
            _ => unreachable!(),
        }
    }
    fn fragment_8(&mut self, depth: usize) {
        if depth >= 15 { return; }
        match self.rand() % 2 {
            0 => self.fragment_169(depth + 1),
            1 => self.fragment_171(depth + 1),
            _ => unreachable!(),
        }
    }
    fn fragment_9(&mut self, depth: usize) {
        if depth >= 15 { return; }
        match self.rand() % 4 {
            0 => self.fragment_173(depth + 1),
            1 => self.fragment_175(depth + 1),
            2 => self.fragment_177(depth + 1),
            3 => self.fragment_179(depth + 1),
            _ => unreachable!(),
        }
    }
    fn fragment_10(&mut self, depth: usize) {
        if depth >= 15 { return; }
        match self.rand() % 2 {
            0 => self.fragment_184(depth + 1),
            1 => self.fragment_186(depth + 1),
            _ => unreachable!(),
        }
    }
    fn fragment_11(&mut self, depth: usize) {
        if depth >= 15 { return; }
        self.fragment_187(depth + 1);
        self.fragment_188(depth + 1);
        self.fragment_189(depth + 1);
        self.fragment_190(depth + 1);
        self.fragment_191(depth + 1);
        self.fragment_192(depth + 1);
        self.fragment_193(depth + 1);
        self.fragment_194(depth + 1);
        self.fragment_195(depth + 1);
        self.fragment_196(depth + 1);
        self.fragment_197(depth + 1);
        self.fragment_198(depth + 1);
    }
    fn fragment_12(&mut self, depth: usize) {
        if depth >= 15 { return; }
        match self.rand() % 2 {
            0 => self.fragment_214(depth + 1),
            1 => self.fragment_229(depth + 1),
            _ => unreachable!(),
        }
    }
    fn fragment_13(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 0;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 0);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_14(&mut self, depth: usize) {
        if depth >= 15 { return; }
        match self.rand() % 62 {
            0 => self.fragment_233(depth + 1),
            1 => self.fragment_235(depth + 1),
            2 => self.fragment_237(depth + 1),
            3 => self.fragment_239(depth + 1),
            4 => self.fragment_241(depth + 1),
            5 => self.fragment_243(depth + 1),
            6 => self.fragment_245(depth + 1),
            7 => self.fragment_247(depth + 1),
            8 => self.fragment_249(depth + 1),
            9 => self.fragment_251(depth + 1),
            10 => self.fragment_253(depth + 1),
            11 => self.fragment_255(depth + 1),
            12 => self.fragment_257(depth + 1),
            13 => self.fragment_259(depth + 1),
            14 => self.fragment_261(depth + 1),
            15 => self.fragment_263(depth + 1),
            16 => self.fragment_265(depth + 1),
            17 => self.fragment_267(depth + 1),
            18 => self.fragment_269(depth + 1),
            19 => self.fragment_271(depth + 1),
            20 => self.fragment_273(depth + 1),
            21 => self.fragment_275(depth + 1),
            22 => self.fragment_277(depth + 1),
            23 => self.fragment_279(depth + 1),
            24 => self.fragment_281(depth + 1),
            25 => self.fragment_283(depth + 1),
            26 => self.fragment_285(depth + 1),
            27 => self.fragment_287(depth + 1),
            28 => self.fragment_289(depth + 1),
            29 => self.fragment_291(depth + 1),
            30 => self.fragment_293(depth + 1),
            31 => self.fragment_295(depth + 1),
            32 => self.fragment_297(depth + 1),
            33 => self.fragment_299(depth + 1),
            34 => self.fragment_301(depth + 1),
            35 => self.fragment_303(depth + 1),
            36 => self.fragment_305(depth + 1),
            37 => self.fragment_307(depth + 1),
            38 => self.fragment_309(depth + 1),
            39 => self.fragment_311(depth + 1),
            40 => self.fragment_313(depth + 1),
            41 => self.fragment_315(depth + 1),
            42 => self.fragment_317(depth + 1),
            43 => self.fragment_319(depth + 1),
            44 => self.fragment_321(depth + 1),
            45 => self.fragment_323(depth + 1),
            46 => self.fragment_325(depth + 1),
            47 => self.fragment_327(depth + 1),
            48 => self.fragment_329(depth + 1),
            49 => self.fragment_331(depth + 1),
            50 => self.fragment_333(depth + 1),
            51 => self.fragment_335(depth + 1),
            52 => self.fragment_337(depth + 1),
            53 => self.fragment_339(depth + 1),
            54 => self.fragment_341(depth + 1),
            55 => self.fragment_343(depth + 1),
            56 => self.fragment_345(depth + 1),
            57 => self.fragment_347(depth + 1),
            58 => self.fragment_349(depth + 1),
            59 => self.fragment_351(depth + 1),
            60 => self.fragment_353(depth + 1),
            61 => self.fragment_355(depth + 1),
            _ => unreachable!(),
        }
    }
    fn fragment_15(&mut self, depth: usize) {
        if depth >= 15 { return; }
        self.fragment_356(depth + 1);
        self.fragment_357(depth + 1);
        self.fragment_358(depth + 1);
        self.fragment_359(depth + 1);
        self.fragment_360(depth + 1);
        self.fragment_361(depth + 1);
        self.fragment_362(depth + 1);
        self.fragment_363(depth + 1);
        self.fragment_364(depth + 1);
        self.fragment_365(depth + 1);
        self.fragment_366(depth + 1);
        self.fragment_367(depth + 1);
    }
    fn fragment_16(&mut self, depth: usize) {
        if depth >= 15 { return; }
        self.fragment_369(depth + 1);
        self.fragment_370(depth + 1);
        self.fragment_371(depth + 1);
        self.fragment_372(depth + 1);
        self.fragment_373(depth + 1);
        self.fragment_374(depth + 1);
        self.fragment_375(depth + 1);
        self.fragment_376(depth + 1);
        self.fragment_377(depth + 1);
        self.fragment_378(depth + 1);
        self.fragment_379(depth + 1);
        self.fragment_380(depth + 1);
    }
    fn fragment_17(&mut self, depth: usize) {
        if depth >= 15 { return; }
        self.fragment_382(depth + 1);
        self.fragment_383(depth + 1);
        self.fragment_384(depth + 1);
        self.fragment_385(depth + 1);
        self.fragment_386(depth + 1);
        self.fragment_387(depth + 1);
    }
    fn fragment_18(&mut self, depth: usize) {
        if depth >= 15 { return; }
        match self.rand() % 15 {
            0 => self.fragment_390(depth + 1),
            1 => self.fragment_392(depth + 1),
            2 => self.fragment_394(depth + 1),
            3 => self.fragment_396(depth + 1),
            4 => self.fragment_398(depth + 1),
            5 => self.fragment_400(depth + 1),
            6 => self.fragment_402(depth + 1),
            7 => self.fragment_404(depth + 1),
            8 => self.fragment_406(depth + 1),
            9 => self.fragment_408(depth + 1),
            10 => self.fragment_410(depth + 1),
            11 => self.fragment_412(depth + 1),
            12 => self.fragment_414(depth + 1),
            13 => self.fragment_416(depth + 1),
            14 => self.fragment_418(depth + 1),
            _ => unreachable!(),
        }
    }
    fn fragment_19(&mut self, depth: usize) {
        if depth >= 15 { return; }
        match self.rand() % 2 {
            0 => self.fragment_425(depth + 1),
            1 => self.fragment_427(depth + 1),
            _ => unreachable!(),
        }
    }
    fn fragment_20(&mut self, depth: usize) {
        if depth >= 15 { return; }
        self.fragment_428(depth + 1);
        self.fragment_429(depth + 1);
        self.fragment_430(depth + 1);
        self.fragment_431(depth + 1);
    }
    fn fragment_21(&mut self, depth: usize) {
        if depth >= 15 { return; }
        match self.rand() % 2 {
            0 => self.fragment_441(depth + 1),
            1 => self.fragment_446(depth + 1),
            _ => unreachable!(),
        }
    }
    fn fragment_22(&mut self, depth: usize) {
        if depth >= 15 { return; }
        match self.rand() % 4 {
            0 => self.fragment_455(depth + 1),
            1 => self.fragment_462(depth + 1),
            2 => self.fragment_467(depth + 1),
            3 => self.fragment_469(depth + 1),
            _ => unreachable!(),
        }
    }
    fn fragment_23(&mut self, depth: usize) {
        if depth >= 15 { return; }
        match self.rand() % 2 {
            0 => self.fragment_480(depth + 1),
            1 => self.fragment_487(depth + 1),
            _ => unreachable!(),
        }
    }
    fn fragment_24(&mut self, depth: usize) {
        if depth >= 15 { return; }
        self.fragment_488(depth + 1);
        self.fragment_489(depth + 1);
        self.fragment_490(depth + 1);
        self.fragment_491(depth + 1);
        self.fragment_492(depth + 1);
        self.fragment_493(depth + 1);
        self.fragment_494(depth + 1);
        self.fragment_495(depth + 1);
    }
    fn fragment_25(&mut self, depth: usize) {
        if depth >= 15 { return; }
        self.fragment_382(depth + 1);
        self.fragment_383(depth + 1);
        self.fragment_384(depth + 1);
        self.fragment_385(depth + 1);
        self.fragment_386(depth + 1);
        self.fragment_387(depth + 1);
    }
    fn fragment_26(&mut self, depth: usize) {
        if depth >= 15 { return; }
        match self.rand() % 11 {
            0 => self.fragment_500(depth + 1),
            1 => self.fragment_502(depth + 1),
            2 => self.fragment_504(depth + 1),
            3 => self.fragment_506(depth + 1),
            4 => self.fragment_508(depth + 1),
            5 => self.fragment_510(depth + 1),
            6 => self.fragment_512(depth + 1),
            7 => self.fragment_514(depth + 1),
            8 => self.fragment_516(depth + 1),
            9 => self.fragment_518(depth + 1),
            10 => self.fragment_520(depth + 1),
            _ => unreachable!(),
        }
    }
    fn fragment_27(&mut self, depth: usize) {
        if depth >= 15 { return; }
        match self.rand() % 2 {
            0 => self.fragment_522(depth + 1),
            1 => self.fragment_524(depth + 1),
            _ => unreachable!(),
        }
    }
    fn fragment_28(&mut self, depth: usize) {
        if depth >= 15 { return; }
        match self.rand() % 2 {
            0 => self.fragment_526(depth + 1),
            1 => self.fragment_528(depth + 1),
            _ => unreachable!(),
        }
    }
    fn fragment_29(&mut self, depth: usize) {
        if depth >= 15 { return; }
        self.fragment_488(depth + 1);
        self.fragment_489(depth + 1);
        self.fragment_490(depth + 1);
        self.fragment_491(depth + 1);
        self.fragment_492(depth + 1);
        self.fragment_493(depth + 1);
        self.fragment_494(depth + 1);
        self.fragment_495(depth + 1);
    }
    fn fragment_30(&mut self, depth: usize) {
        if depth >= 15 { return; }
        self.fragment_488(depth + 1);
        self.fragment_489(depth + 1);
        self.fragment_490(depth + 1);
        self.fragment_491(depth + 1);
        self.fragment_492(depth + 1);
        self.fragment_493(depth + 1);
        self.fragment_494(depth + 1);
        self.fragment_495(depth + 1);
    }
    fn fragment_31(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([40].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_32(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([32].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_33(&mut self, depth: usize) {
        if depth >= 15 { return; }
        match self.rand() % 2 {
            0 => self.fragment_441(depth + 1),
            1 => self.fragment_446(depth + 1),
            _ => unreachable!(),
        }
    }
    fn fragment_34(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([32].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_35(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([41].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_36(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([32].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_37(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([123].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_38(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([32].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_39(&mut self, depth: usize) {
        if depth >= 15 { return; }
        match self.rand() % 4 {
            0 => self.fragment_145(depth + 1),
            1 => self.fragment_150(depth + 1),
            2 => self.fragment_155(depth + 1),
            3 => self.fragment_164(depth + 1),
            _ => unreachable!(),
        }
    }
    fn fragment_40(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([32].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_41(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([125].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_42(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([32].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_43(&mut self, depth: usize) {
        if depth >= 15 { return; }
        self.fragment_31(depth + 1);
        self.fragment_32(depth + 1);
        self.fragment_33(depth + 1);
        self.fragment_34(depth + 1);
        self.fragment_35(depth + 1);
        self.fragment_36(depth + 1);
        self.fragment_37(depth + 1);
        self.fragment_38(depth + 1);
        self.fragment_39(depth + 1);
        self.fragment_40(depth + 1);
        self.fragment_41(depth + 1);
        self.fragment_42(depth + 1);
    }
    fn fragment_44(&mut self, depth: usize) {
        if depth >= 15 { return; }
        match self.rand() % 2 {
            0 => self.fragment_526(depth + 1),
            1 => self.fragment_528(depth + 1),
            _ => unreachable!(),
        }
    }
    fn fragment_45(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([32].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_46(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([44].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_47(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([32].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_48(&mut self, depth: usize) {
        if depth >= 15 { return; }
        match self.rand() % 2 {
            0 => self.fragment_50(depth + 1),
            1 => self.fragment_52(depth + 1),
            _ => unreachable!(),
        }
    }
    fn fragment_49(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([32].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_50(&mut self, depth: usize) {
        if depth >= 15 { return; }
        self.fragment_44(depth + 1);
        self.fragment_45(depth + 1);
        self.fragment_46(depth + 1);
        self.fragment_47(depth + 1);
        self.fragment_48(depth + 1);
        self.fragment_49(depth + 1);
    }
    fn fragment_51(&mut self, depth: usize) {
        if depth >= 15 { return; }
        match self.rand() % 2 {
            0 => self.fragment_526(depth + 1),
            1 => self.fragment_528(depth + 1),
            _ => unreachable!(),
        }
    }
    fn fragment_52(&mut self, depth: usize) {
        if depth >= 15 { return; }
        match self.rand() % 2 {
            0 => self.fragment_526(depth + 1),
            1 => self.fragment_528(depth + 1),
            _ => unreachable!(),
        }
    }
    fn fragment_53(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 10;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([101, 115, 108, 122, 88, 74, 101, 65, 73, 109].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 10);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_54(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 10;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([101, 115, 108, 122, 88, 74, 101, 65, 73, 109].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 10);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_55(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 19;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([115, 54, 52, 78, 115, 100, 86, 108, 68, 68, 52, 118, 113, 109, 79, 121, 76, 115, 54].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 19);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_56(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 19;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([115, 54, 52, 78, 115, 100, 86, 108, 68, 68, 52, 118, 113, 109, 79, 121, 76, 115, 54].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 19);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_57(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 19;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([119, 89, 73, 113, 117, 77, 52, 110, 95, 53, 87, 110, 111, 68, 105, 73, 77, 69, 75].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 19);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_58(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 19;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([119, 89, 73, 113, 117, 77, 52, 110, 95, 53, 87, 110, 111, 68, 105, 73, 77, 69, 75].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 19);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_59(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 9;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([99, 50, 83, 66, 119, 52, 65, 95, 116].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 9);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_60(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 9;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([99, 50, 83, 66, 119, 52, 65, 95, 116].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 9);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_61(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 14;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([121, 51, 69, 76, 79, 84, 103, 95, 99, 49, 119, 105, 80, 122].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 14);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_62(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 14;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([121, 51, 69, 76, 79, 84, 103, 95, 99, 49, 119, 105, 80, 122].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 14);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_63(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 4;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([118, 66, 120, 121].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 4);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_64(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 4;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([118, 66, 120, 121].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 4);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_65(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 17;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([117, 75, 49, 108, 71, 72, 120, 71, 67, 77, 111, 71, 118, 85, 72, 53, 89].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 17);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_66(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 17;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([117, 75, 49, 108, 71, 72, 120, 71, 67, 77, 111, 71, 118, 85, 72, 53, 89].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 17);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_67(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 19;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([115, 65, 119, 69, 57, 113, 109, 52, 95, 102, 97, 85, 81, 66, 111, 53, 79, 82, 117].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 19);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_68(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 19;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([115, 65, 119, 69, 57, 113, 109, 52, 95, 102, 97, 85, 81, 66, 111, 53, 79, 82, 117].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 19);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_69(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 3;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([117, 76, 107].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 3);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_70(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 3;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([117, 76, 107].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 3);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_71(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 10;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([116, 108, 119, 117, 88, 67, 104, 98, 50, 67].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 10);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_72(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 10;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([116, 108, 119, 117, 88, 67, 104, 98, 50, 67].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 10);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_73(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 11;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([102, 85, 106, 104, 85, 75, 67, 86, 117, 97, 114].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 11);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_74(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 11;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([102, 85, 106, 104, 85, 75, 67, 86, 117, 97, 114].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 11);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_75(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 13;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([122, 106, 78, 107, 114, 49, 86, 50, 55, 89, 75, 115, 57].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 13);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_76(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 13;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([122, 106, 78, 107, 114, 49, 86, 50, 55, 89, 75, 115, 57].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 13);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_77(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 18;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([106, 112, 67, 99, 106, 111, 55, 54, 67, 116, 106, 97, 103, 100, 88, 117, 76, 66].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 18);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_78(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 18;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([106, 112, 67, 99, 106, 111, 55, 54, 67, 116, 106, 97, 103, 100, 88, 117, 76, 66].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 18);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_79(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 14;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([115, 112, 87, 86, 118, 69, 73, 118, 101, 53, 57, 53, 90, 69].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 14);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_80(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 14;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([115, 112, 87, 86, 118, 69, 73, 118, 101, 53, 57, 53, 90, 69].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 14);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_81(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 8;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([108, 111, 86, 121, 68, 85, 74, 54].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 8);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_82(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 8;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([108, 111, 86, 121, 68, 85, 74, 54].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 8);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_83(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 5;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([98, 111, 117, 110, 100].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 5);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_84(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 5;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([98, 111, 117, 110, 100].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 5);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_85(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 4;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([102, 114, 101, 101].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 4);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_86(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 4;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([102, 114, 101, 101].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 4);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_87(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 9;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([97, 103, 103, 114, 101, 103, 97, 116, 101].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 9);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_88(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 9;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([97, 103, 103, 114, 101, 103, 97, 116, 101].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 9);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_89(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 7;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([115, 117, 109, 109, 97, 114, 121].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 7);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_90(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 7;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([115, 117, 109, 109, 97, 114, 121].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 7);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_91(&mut self, depth: usize) {
        if depth >= 15 { return; }
        match self.rand() % 15 {
            0 => self.fragment_54(depth + 1),
            1 => self.fragment_56(depth + 1),
            2 => self.fragment_58(depth + 1),
            3 => self.fragment_60(depth + 1),
            4 => self.fragment_62(depth + 1),
            5 => self.fragment_64(depth + 1),
            6 => self.fragment_66(depth + 1),
            7 => self.fragment_68(depth + 1),
            8 => self.fragment_70(depth + 1),
            9 => self.fragment_72(depth + 1),
            10 => self.fragment_74(depth + 1),
            11 => self.fragment_76(depth + 1),
            12 => self.fragment_78(depth + 1),
            13 => self.fragment_80(depth + 1),
            14 => self.fragment_82(depth + 1),
            _ => unreachable!(),
        }
    }
    fn fragment_92(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([32].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_93(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([40].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_94(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([32].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_95(&mut self, depth: usize) {
        if depth >= 15 { return; }
        match self.rand() % 2 {
            0 => self.fragment_425(depth + 1),
            1 => self.fragment_427(depth + 1),
            _ => unreachable!(),
        }
    }
    fn fragment_96(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([32].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_97(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([41].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_98(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([32].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_99(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([58].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_100(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([32].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_101(&mut self, depth: usize) {
        if depth >= 15 { return; }
        match self.rand() % 4 {
            0 => self.fragment_145(depth + 1),
            1 => self.fragment_150(depth + 1),
            2 => self.fragment_155(depth + 1),
            3 => self.fragment_164(depth + 1),
            _ => unreachable!(),
        }
    }
    fn fragment_102(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([32].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_103(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([46].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_104(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([32].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_105(&mut self, depth: usize) {
        if depth >= 15 { return; }
        self.fragment_91(depth + 1);
        self.fragment_92(depth + 1);
        self.fragment_93(depth + 1);
        self.fragment_94(depth + 1);
        self.fragment_95(depth + 1);
        self.fragment_96(depth + 1);
        self.fragment_97(depth + 1);
        self.fragment_98(depth + 1);
        self.fragment_99(depth + 1);
        self.fragment_100(depth + 1);
        self.fragment_101(depth + 1);
        self.fragment_102(depth + 1);
        self.fragment_103(depth + 1);
        self.fragment_104(depth + 1);
    }
    fn fragment_106(&mut self, depth: usize) {
        if depth >= 15 { return; }
        self.fragment_91(depth + 1);
        self.fragment_92(depth + 1);
        self.fragment_93(depth + 1);
        self.fragment_94(depth + 1);
        self.fragment_95(depth + 1);
        self.fragment_96(depth + 1);
        self.fragment_97(depth + 1);
        self.fragment_98(depth + 1);
        self.fragment_99(depth + 1);
        self.fragment_100(depth + 1);
        self.fragment_101(depth + 1);
        self.fragment_102(depth + 1);
        self.fragment_103(depth + 1);
        self.fragment_104(depth + 1);
    }
    fn fragment_107(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([32].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_108(&mut self, depth: usize) {
        if depth >= 15 { return; }
        match self.rand() % 2 {
            0 => self.fragment_110(depth + 1),
            1 => self.fragment_112(depth + 1),
            _ => unreachable!(),
        }
    }
    fn fragment_109(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([32].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_110(&mut self, depth: usize) {
        if depth >= 15 { return; }
        self.fragment_106(depth + 1);
        self.fragment_107(depth + 1);
        self.fragment_108(depth + 1);
        self.fragment_109(depth + 1);
    }
    fn fragment_111(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 0;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 0);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_112(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 0;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 0);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_113(&mut self, depth: usize) {
        if depth >= 15 { return; }
        match self.rand() % 2 {
            0 => self.fragment_526(depth + 1),
            1 => self.fragment_528(depth + 1),
            _ => unreachable!(),
        }
    }
    fn fragment_114(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([32].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_115(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([61].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_116(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([32].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_117(&mut self, depth: usize) {
        if depth >= 15 { return; }
        match self.rand() % 2 {
            0 => self.fragment_526(depth + 1),
            1 => self.fragment_528(depth + 1),
            _ => unreachable!(),
        }
    }
    fn fragment_118(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([32].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_119(&mut self, depth: usize) {
        if depth >= 15 { return; }
        self.fragment_113(depth + 1);
        self.fragment_114(depth + 1);
        self.fragment_115(depth + 1);
        self.fragment_116(depth + 1);
        self.fragment_117(depth + 1);
        self.fragment_118(depth + 1);
    }
    fn fragment_120(&mut self, depth: usize) {
        if depth >= 15 { return; }
        match self.rand() % 2 {
            0 => self.fragment_526(depth + 1),
            1 => self.fragment_528(depth + 1),
            _ => unreachable!(),
        }
    }
    fn fragment_121(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([32].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_122(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 2;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([33, 61].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 2);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_123(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([32].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_124(&mut self, depth: usize) {
        if depth >= 15 { return; }
        match self.rand() % 2 {
            0 => self.fragment_526(depth + 1),
            1 => self.fragment_528(depth + 1),
            _ => unreachable!(),
        }
    }
    fn fragment_125(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([32].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_126(&mut self, depth: usize) {
        if depth >= 15 { return; }
        self.fragment_120(depth + 1);
        self.fragment_121(depth + 1);
        self.fragment_122(depth + 1);
        self.fragment_123(depth + 1);
        self.fragment_124(depth + 1);
        self.fragment_125(depth + 1);
    }
    fn fragment_127(&mut self, depth: usize) {
        if depth >= 15 { return; }
        match self.rand() % 2 {
            0 => self.fragment_526(depth + 1),
            1 => self.fragment_528(depth + 1),
            _ => unreachable!(),
        }
    }
    fn fragment_128(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([32].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_129(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([60].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_130(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([32].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_131(&mut self, depth: usize) {
        if depth >= 15 { return; }
        match self.rand() % 2 {
            0 => self.fragment_526(depth + 1),
            1 => self.fragment_528(depth + 1),
            _ => unreachable!(),
        }
    }
    fn fragment_132(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([32].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_133(&mut self, depth: usize) {
        if depth >= 15 { return; }
        self.fragment_127(depth + 1);
        self.fragment_128(depth + 1);
        self.fragment_129(depth + 1);
        self.fragment_130(depth + 1);
        self.fragment_131(depth + 1);
        self.fragment_132(depth + 1);
    }
    fn fragment_134(&mut self, depth: usize) {
        if depth >= 15 { return; }
        match self.rand() % 2 {
            0 => self.fragment_526(depth + 1),
            1 => self.fragment_528(depth + 1),
            _ => unreachable!(),
        }
    }
    fn fragment_135(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([32].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_136(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([62].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_137(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([32].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_138(&mut self, depth: usize) {
        if depth >= 15 { return; }
        match self.rand() % 2 {
            0 => self.fragment_526(depth + 1),
            1 => self.fragment_528(depth + 1),
            _ => unreachable!(),
        }
    }
    fn fragment_139(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([32].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_140(&mut self, depth: usize) {
        if depth >= 15 { return; }
        self.fragment_134(depth + 1);
        self.fragment_135(depth + 1);
        self.fragment_136(depth + 1);
        self.fragment_137(depth + 1);
        self.fragment_138(depth + 1);
        self.fragment_139(depth + 1);
    }
    fn fragment_141(&mut self, depth: usize) {
        if depth >= 15 { return; }
        match self.rand() % 4 {
            0 => self.fragment_119(depth + 1),
            1 => self.fragment_126(depth + 1),
            2 => self.fragment_133(depth + 1),
            3 => self.fragment_140(depth + 1),
            _ => unreachable!(),
        }
    }
    fn fragment_142(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([32].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_143(&mut self, depth: usize) {
        if depth >= 15 { return; }
        match self.rand() % 2 {
            0 => self.fragment_169(depth + 1),
            1 => self.fragment_171(depth + 1),
            _ => unreachable!(),
        }
    }
    fn fragment_144(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([32].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_145(&mut self, depth: usize) {
        if depth >= 15 { return; }
        self.fragment_141(depth + 1);
        self.fragment_142(depth + 1);
        self.fragment_143(depth + 1);
        self.fragment_144(depth + 1);
    }
    fn fragment_146(&mut self, depth: usize) {
        if depth >= 15 { return; }
        self.fragment_488(depth + 1);
        self.fragment_489(depth + 1);
        self.fragment_490(depth + 1);
        self.fragment_491(depth + 1);
        self.fragment_492(depth + 1);
        self.fragment_493(depth + 1);
        self.fragment_494(depth + 1);
        self.fragment_495(depth + 1);
    }
    fn fragment_147(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([32].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_148(&mut self, depth: usize) {
        if depth >= 15 { return; }
        match self.rand() % 2 {
            0 => self.fragment_169(depth + 1),
            1 => self.fragment_171(depth + 1),
            _ => unreachable!(),
        }
    }
    fn fragment_149(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([32].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_150(&mut self, depth: usize) {
        if depth >= 15 { return; }
        self.fragment_146(depth + 1);
        self.fragment_147(depth + 1);
        self.fragment_148(depth + 1);
        self.fragment_149(depth + 1);
    }
    fn fragment_151(&mut self, depth: usize) {
        if depth >= 15 { return; }
        self.fragment_428(depth + 1);
        self.fragment_429(depth + 1);
        self.fragment_430(depth + 1);
        self.fragment_431(depth + 1);
    }
    fn fragment_152(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([32].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_153(&mut self, depth: usize) {
        if depth >= 15 { return; }
        match self.rand() % 2 {
            0 => self.fragment_169(depth + 1),
            1 => self.fragment_171(depth + 1),
            _ => unreachable!(),
        }
    }
    fn fragment_154(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([32].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_155(&mut self, depth: usize) {
        if depth >= 15 { return; }
        self.fragment_151(depth + 1);
        self.fragment_152(depth + 1);
        self.fragment_153(depth + 1);
        self.fragment_154(depth + 1);
    }
    fn fragment_156(&mut self, depth: usize) {
        if depth >= 15 { return; }
        self.fragment_488(depth + 1);
        self.fragment_489(depth + 1);
        self.fragment_490(depth + 1);
        self.fragment_491(depth + 1);
        self.fragment_492(depth + 1);
        self.fragment_493(depth + 1);
        self.fragment_494(depth + 1);
        self.fragment_495(depth + 1);
    }
    fn fragment_157(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([32].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_158(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 4;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([111, 118, 101, 114].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 4);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_159(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([32].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_160(&mut self, depth: usize) {
        if depth >= 15 { return; }
        match self.rand() % 2 {
            0 => self.fragment_30(depth + 1),
            1 => self.fragment_43(depth + 1),
            _ => unreachable!(),
        }
    }
    fn fragment_161(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([32].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_162(&mut self, depth: usize) {
        if depth >= 15 { return; }
        match self.rand() % 2 {
            0 => self.fragment_169(depth + 1),
            1 => self.fragment_171(depth + 1),
            _ => unreachable!(),
        }
    }
    fn fragment_163(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([32].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_164(&mut self, depth: usize) {
        if depth >= 15 { return; }
        self.fragment_156(depth + 1);
        self.fragment_157(depth + 1);
        self.fragment_158(depth + 1);
        self.fragment_159(depth + 1);
        self.fragment_160(depth + 1);
        self.fragment_161(depth + 1);
        self.fragment_162(depth + 1);
        self.fragment_163(depth + 1);
    }
    fn fragment_165(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([44].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_166(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([32].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_167(&mut self, depth: usize) {
        if depth >= 15 { return; }
        match self.rand() % 4 {
            0 => self.fragment_145(depth + 1),
            1 => self.fragment_150(depth + 1),
            2 => self.fragment_155(depth + 1),
            3 => self.fragment_164(depth + 1),
            _ => unreachable!(),
        }
    }
    fn fragment_168(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([32].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_169(&mut self, depth: usize) {
        if depth >= 15 { return; }
        self.fragment_165(depth + 1);
        self.fragment_166(depth + 1);
        self.fragment_167(depth + 1);
        self.fragment_168(depth + 1);
    }
    fn fragment_170(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 0;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 0);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_171(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 0;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 0);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_172(&mut self, depth: usize) {
        if depth >= 15 { return; }
        self.fragment_187(depth + 1);
        self.fragment_188(depth + 1);
        self.fragment_189(depth + 1);
        self.fragment_190(depth + 1);
        self.fragment_191(depth + 1);
        self.fragment_192(depth + 1);
        self.fragment_193(depth + 1);
        self.fragment_194(depth + 1);
        self.fragment_195(depth + 1);
        self.fragment_196(depth + 1);
        self.fragment_197(depth + 1);
        self.fragment_198(depth + 1);
    }
    fn fragment_173(&mut self, depth: usize) {
        if depth >= 15 { return; }
        self.fragment_187(depth + 1);
        self.fragment_188(depth + 1);
        self.fragment_189(depth + 1);
        self.fragment_190(depth + 1);
        self.fragment_191(depth + 1);
        self.fragment_192(depth + 1);
        self.fragment_193(depth + 1);
        self.fragment_194(depth + 1);
        self.fragment_195(depth + 1);
        self.fragment_196(depth + 1);
        self.fragment_197(depth + 1);
        self.fragment_198(depth + 1);
    }
    fn fragment_174(&mut self, depth: usize) {
        if depth >= 15 { return; }
        self.fragment_356(depth + 1);
        self.fragment_357(depth + 1);
        self.fragment_358(depth + 1);
        self.fragment_359(depth + 1);
        self.fragment_360(depth + 1);
        self.fragment_361(depth + 1);
        self.fragment_362(depth + 1);
        self.fragment_363(depth + 1);
        self.fragment_364(depth + 1);
        self.fragment_365(depth + 1);
        self.fragment_366(depth + 1);
        self.fragment_367(depth + 1);
    }
    fn fragment_175(&mut self, depth: usize) {
        if depth >= 15 { return; }
        self.fragment_356(depth + 1);
        self.fragment_357(depth + 1);
        self.fragment_358(depth + 1);
        self.fragment_359(depth + 1);
        self.fragment_360(depth + 1);
        self.fragment_361(depth + 1);
        self.fragment_362(depth + 1);
        self.fragment_363(depth + 1);
        self.fragment_364(depth + 1);
        self.fragment_365(depth + 1);
        self.fragment_366(depth + 1);
        self.fragment_367(depth + 1);
    }
    fn fragment_176(&mut self, depth: usize) {
        if depth >= 15 { return; }
        match self.rand() % 2 {
            0 => self.fragment_214(depth + 1),
            1 => self.fragment_229(depth + 1),
            _ => unreachable!(),
        }
    }
    fn fragment_177(&mut self, depth: usize) {
        if depth >= 15 { return; }
        match self.rand() % 2 {
            0 => self.fragment_214(depth + 1),
            1 => self.fragment_229(depth + 1),
            _ => unreachable!(),
        }
    }
    fn fragment_178(&mut self, depth: usize) {
        if depth >= 15 { return; }
        self.fragment_369(depth + 1);
        self.fragment_370(depth + 1);
        self.fragment_371(depth + 1);
        self.fragment_372(depth + 1);
        self.fragment_373(depth + 1);
        self.fragment_374(depth + 1);
        self.fragment_375(depth + 1);
        self.fragment_376(depth + 1);
        self.fragment_377(depth + 1);
        self.fragment_378(depth + 1);
        self.fragment_379(depth + 1);
        self.fragment_380(depth + 1);
    }
    fn fragment_179(&mut self, depth: usize) {
        if depth >= 15 { return; }
        self.fragment_369(depth + 1);
        self.fragment_370(depth + 1);
        self.fragment_371(depth + 1);
        self.fragment_372(depth + 1);
        self.fragment_373(depth + 1);
        self.fragment_374(depth + 1);
        self.fragment_375(depth + 1);
        self.fragment_376(depth + 1);
        self.fragment_377(depth + 1);
        self.fragment_378(depth + 1);
        self.fragment_379(depth + 1);
        self.fragment_380(depth + 1);
    }
    fn fragment_180(&mut self, depth: usize) {
        if depth >= 15 { return; }
        match self.rand() % 4 {
            0 => self.fragment_173(depth + 1),
            1 => self.fragment_175(depth + 1),
            2 => self.fragment_177(depth + 1),
            3 => self.fragment_179(depth + 1),
            _ => unreachable!(),
        }
    }
    fn fragment_181(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([32].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_182(&mut self, depth: usize) {
        if depth >= 15 { return; }
        match self.rand() % 2 {
            0 => self.fragment_184(depth + 1),
            1 => self.fragment_186(depth + 1),
            _ => unreachable!(),
        }
    }
    fn fragment_183(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([32].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_184(&mut self, depth: usize) {
        if depth >= 15 { return; }
        self.fragment_180(depth + 1);
        self.fragment_181(depth + 1);
        self.fragment_182(depth + 1);
        self.fragment_183(depth + 1);
    }
    fn fragment_185(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 0;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 0);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_186(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 0;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 0);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_187(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 7;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([35, 101, 120, 112, 111, 114, 116].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 7);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_188(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([32].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_189(&mut self, depth: usize) {
        if depth >= 15 { return; }
        match self.rand() % 15 {
            0 => self.fragment_54(depth + 1),
            1 => self.fragment_56(depth + 1),
            2 => self.fragment_58(depth + 1),
            3 => self.fragment_60(depth + 1),
            4 => self.fragment_62(depth + 1),
            5 => self.fragment_64(depth + 1),
            6 => self.fragment_66(depth + 1),
            7 => self.fragment_68(depth + 1),
            8 => self.fragment_70(depth + 1),
            9 => self.fragment_72(depth + 1),
            10 => self.fragment_74(depth + 1),
            11 => self.fragment_76(depth + 1),
            12 => self.fragment_78(depth + 1),
            13 => self.fragment_80(depth + 1),
            14 => self.fragment_82(depth + 1),
            _ => unreachable!(),
        }
    }
    fn fragment_190(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([32].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_191(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([40].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_192(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([32].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_193(&mut self, depth: usize) {
        if depth >= 15 { return; }
        match self.rand() % 2 {
            0 => self.fragment_441(depth + 1),
            1 => self.fragment_446(depth + 1),
            _ => unreachable!(),
        }
    }
    fn fragment_194(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([32].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_195(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([41].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_196(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([32].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_197(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([10].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_198(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([32].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_199(&mut self, depth: usize) {
        if depth >= 15 { return; }
        self.fragment_187(depth + 1);
        self.fragment_188(depth + 1);
        self.fragment_189(depth + 1);
        self.fragment_190(depth + 1);
        self.fragment_191(depth + 1);
        self.fragment_192(depth + 1);
        self.fragment_193(depth + 1);
        self.fragment_194(depth + 1);
        self.fragment_195(depth + 1);
        self.fragment_196(depth + 1);
        self.fragment_197(depth + 1);
        self.fragment_198(depth + 1);
    }
    fn fragment_200(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 8;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([35, 102, 117, 110, 99, 116, 111, 114].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 8);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_201(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([32].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_202(&mut self, depth: usize) {
        if depth >= 15 { return; }
        match self.rand() % 15 {
            0 => self.fragment_54(depth + 1),
            1 => self.fragment_56(depth + 1),
            2 => self.fragment_58(depth + 1),
            3 => self.fragment_60(depth + 1),
            4 => self.fragment_62(depth + 1),
            5 => self.fragment_64(depth + 1),
            6 => self.fragment_66(depth + 1),
            7 => self.fragment_68(depth + 1),
            8 => self.fragment_70(depth + 1),
            9 => self.fragment_72(depth + 1),
            10 => self.fragment_74(depth + 1),
            11 => self.fragment_76(depth + 1),
            12 => self.fragment_78(depth + 1),
            13 => self.fragment_80(depth + 1),
            14 => self.fragment_82(depth + 1),
            _ => unreachable!(),
        }
    }
    fn fragment_203(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([32].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_204(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([40].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_205(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([32].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_206(&mut self, depth: usize) {
        if depth >= 15 { return; }
        match self.rand() % 2 {
            0 => self.fragment_480(depth + 1),
            1 => self.fragment_487(depth + 1),
            _ => unreachable!(),
        }
    }
    fn fragment_207(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([32].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_208(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([41].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_209(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([32].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_210(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 7;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([116, 114, 105, 118, 105, 97, 108].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 7);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_211(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([32].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_212(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([10].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_213(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([32].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_214(&mut self, depth: usize) {
        if depth >= 15 { return; }
        self.fragment_200(depth + 1);
        self.fragment_201(depth + 1);
        self.fragment_202(depth + 1);
        self.fragment_203(depth + 1);
        self.fragment_204(depth + 1);
        self.fragment_205(depth + 1);
        self.fragment_206(depth + 1);
        self.fragment_207(depth + 1);
        self.fragment_208(depth + 1);
        self.fragment_209(depth + 1);
        self.fragment_210(depth + 1);
        self.fragment_211(depth + 1);
        self.fragment_212(depth + 1);
        self.fragment_213(depth + 1);
    }
    fn fragment_215(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 8;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([35, 102, 117, 110, 99, 116, 111, 114].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 8);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_216(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([32].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_217(&mut self, depth: usize) {
        if depth >= 15 { return; }
        match self.rand() % 15 {
            0 => self.fragment_54(depth + 1),
            1 => self.fragment_56(depth + 1),
            2 => self.fragment_58(depth + 1),
            3 => self.fragment_60(depth + 1),
            4 => self.fragment_62(depth + 1),
            5 => self.fragment_64(depth + 1),
            6 => self.fragment_66(depth + 1),
            7 => self.fragment_68(depth + 1),
            8 => self.fragment_70(depth + 1),
            9 => self.fragment_72(depth + 1),
            10 => self.fragment_74(depth + 1),
            11 => self.fragment_76(depth + 1),
            12 => self.fragment_78(depth + 1),
            13 => self.fragment_80(depth + 1),
            14 => self.fragment_82(depth + 1),
            _ => unreachable!(),
        }
    }
    fn fragment_218(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([32].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_219(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([40].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_220(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([32].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_221(&mut self, depth: usize) {
        if depth >= 15 { return; }
        match self.rand() % 2 {
            0 => self.fragment_480(depth + 1),
            1 => self.fragment_487(depth + 1),
            _ => unreachable!(),
        }
    }
    fn fragment_222(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([32].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_223(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([41].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_224(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([32].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_225(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 7;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([99, 111, 109, 112, 108, 101, 120].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 7);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_226(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([32].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_227(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([10].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_228(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([32].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_229(&mut self, depth: usize) {
        if depth >= 15 { return; }
        self.fragment_215(depth + 1);
        self.fragment_216(depth + 1);
        self.fragment_217(depth + 1);
        self.fragment_218(depth + 1);
        self.fragment_219(depth + 1);
        self.fragment_220(depth + 1);
        self.fragment_221(depth + 1);
        self.fragment_222(depth + 1);
        self.fragment_223(depth + 1);
        self.fragment_224(depth + 1);
        self.fragment_225(depth + 1);
        self.fragment_226(depth + 1);
        self.fragment_227(depth + 1);
        self.fragment_228(depth + 1);
    }
    fn fragment_230(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 0;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 0);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_231(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 0;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 0);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_232(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([48].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_233(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([48].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_234(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 20;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([51, 57, 56, 49, 53, 54, 50, 57, 53, 56, 51, 57, 57, 52, 49, 49, 52, 52, 49, 56].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 20);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_235(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 20;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([51, 57, 56, 49, 53, 54, 50, 57, 53, 56, 51, 57, 57, 52, 49, 49, 52, 52, 49, 56].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 20);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_236(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 2;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([49, 57].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 2);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_237(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 2;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([49, 57].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 2);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_238(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 15;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([56, 50, 56, 52, 53, 55, 55, 51, 55, 51, 56, 51, 52, 53, 54].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 15);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_239(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 15;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([56, 50, 56, 52, 53, 55, 55, 51, 55, 51, 56, 51, 52, 53, 54].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 15);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_240(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 5;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([52, 57, 48, 57, 50].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 5);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_241(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 5;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([52, 57, 48, 57, 50].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 5);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_242(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 7;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([53, 50, 48, 57, 49, 48, 49].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 7);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_243(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 7;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([53, 50, 48, 57, 49, 48, 49].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 7);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_244(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 5;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([49, 56, 55, 55, 48].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 5);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_245(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 5;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([49, 56, 55, 55, 48].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 5);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_246(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 12;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([55, 52, 50, 50, 52, 49, 51, 50, 55, 56, 48, 48].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 12);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_247(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 12;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([55, 52, 50, 50, 52, 49, 51, 50, 55, 56, 48, 48].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 12);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_248(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 13;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([54, 48, 51, 57, 52, 50, 55, 54, 52, 48, 55, 53, 50].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 13);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_249(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 13;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([54, 48, 51, 57, 52, 50, 55, 54, 52, 48, 55, 53, 50].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 13);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_250(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 6;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([50, 51, 48, 51, 50, 57].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 6);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_251(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 6;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([50, 51, 48, 51, 50, 57].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 6);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_252(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 18;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([55, 53, 48, 55, 50, 50, 49, 57, 48, 50, 49, 55, 52, 54, 50, 55, 57, 55].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 18);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_253(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 18;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([55, 53, 48, 55, 50, 50, 49, 57, 48, 50, 49, 55, 52, 54, 50, 55, 57, 55].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 18);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_254(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 19;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([54, 54, 52, 56, 56, 49, 55, 52, 51, 56, 55, 48, 56, 48, 48, 48, 52, 50, 52].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 19);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_255(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 19;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([54, 54, 52, 56, 56, 49, 55, 52, 51, 56, 55, 48, 56, 48, 48, 48, 52, 50, 52].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 19);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_256(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 13;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([50, 57, 53, 55, 48, 51, 48, 56, 56, 50, 52, 49, 53].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 13);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_257(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 13;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([50, 57, 53, 55, 48, 51, 48, 56, 56, 50, 52, 49, 53].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 13);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_258(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 13;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([57, 53, 54, 50, 57, 51, 51, 54, 56, 57, 53, 54, 52].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 13);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_259(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 13;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([57, 53, 54, 50, 57, 51, 51, 54, 56, 57, 53, 54, 52].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 13);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_260(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 5;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([56, 55, 52, 56, 56].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 5);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_261(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 5;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([56, 55, 52, 56, 56].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 5);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_262(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 10;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([54, 53, 52, 57, 50, 49, 56, 52, 56, 48].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 10);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_263(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 10;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([54, 53, 52, 57, 50, 49, 56, 52, 56, 48].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 10);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_264(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 11;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([48, 52, 52, 48, 53, 55, 55, 51, 51, 55, 52].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 11);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_265(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 11;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([48, 52, 52, 48, 53, 55, 55, 51, 51, 55, 52].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 11);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_266(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 2;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([48, 55].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 2);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_267(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 2;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([48, 55].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 2);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_268(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 12;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([48, 55, 54, 55, 51, 51, 48, 50, 54, 49, 51, 54].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 12);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_269(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 12;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([48, 55, 54, 55, 51, 51, 48, 50, 54, 49, 51, 54].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 12);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_270(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 12;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([48, 52, 54, 55, 53, 51, 55, 52, 53, 48, 53, 53].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 12);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_271(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 12;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([48, 52, 54, 55, 53, 51, 55, 52, 53, 48, 53, 53].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 12);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_272(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 5;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([48, 54, 49, 50, 52].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 5);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_273(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 5;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([48, 54, 49, 50, 52].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 5);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_274(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 2;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([48, 55].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 2);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_275(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 2;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([48, 55].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 2);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_276(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 12;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([48, 54, 48, 49, 51, 54, 53, 49, 52, 50, 52, 53].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 12);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_277(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 12;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([48, 54, 48, 49, 51, 54, 53, 49, 52, 50, 52, 53].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 12);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_278(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 14;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([48, 51, 54, 50, 55, 54, 55, 53, 50, 55, 49, 52, 51, 50].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 14);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_279(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 14;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([48, 51, 54, 50, 55, 54, 55, 53, 50, 55, 49, 52, 51, 50].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 14);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_280(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 5;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([48, 50, 49, 48, 52].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 5);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_281(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 5;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([48, 50, 49, 48, 52].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 5);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_282(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 19;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([48, 51, 55, 53, 54, 48, 53, 52, 50, 53, 54, 48, 55, 54, 53, 52, 50, 52, 49].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 19);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_283(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 19;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([48, 51, 55, 53, 54, 48, 53, 52, 50, 53, 54, 48, 55, 54, 53, 52, 50, 52, 49].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 19);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_284(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 8;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([48, 54, 48, 55, 54, 51, 48, 53].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 8);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_285(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 8;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([48, 54, 48, 55, 54, 51, 48, 53].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 8);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_286(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 5;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([48, 49, 48, 53, 49].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 5);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_287(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 5;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([48, 49, 48, 53, 49].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 5);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_288(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 13;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([48, 52, 52, 49, 53, 54, 52, 54, 53, 51, 54, 55, 48].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 13);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_289(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 13;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([48, 52, 52, 49, 53, 54, 52, 54, 53, 51, 54, 55, 48].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 13);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_290(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 18;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([48, 54, 48, 51, 52, 54, 52, 52, 53, 52, 53, 48, 49, 49, 54, 48, 51, 55].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 18);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_291(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 18;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([48, 54, 48, 51, 52, 54, 52, 52, 53, 52, 53, 48, 49, 49, 54, 48, 51, 55].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 18);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_292(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 20;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([48, 55, 52, 51, 54, 54, 51, 54, 48, 51, 51, 53, 52, 53, 52, 54, 54, 49, 53, 53].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 20);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_293(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 20;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([48, 55, 52, 51, 54, 54, 51, 54, 48, 51, 51, 53, 52, 53, 52, 54, 54, 49, 53, 53].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 20);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_294(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 4;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([48, 120, 70, 70].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 4);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_295(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 4;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([48, 120, 70, 70].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 4);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_296(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 14;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([48, 120, 55, 99, 69, 53, 50, 49, 69, 55, 102, 101, 99, 53].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 14);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_297(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 14;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([48, 120, 55, 99, 69, 53, 50, 49, 69, 55, 102, 101, 99, 53].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 14);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_298(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 17;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([48, 120, 70, 70, 99, 102, 70, 69, 52, 66, 98, 50, 70, 53, 51, 68, 97].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 17);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_299(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 17;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([48, 120, 70, 70, 99, 102, 70, 69, 52, 66, 98, 50, 70, 53, 51, 68, 97].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 17);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_300(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 14;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([48, 120, 67, 49, 99, 56, 102, 69, 55, 102, 68, 55, 57, 53].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 14);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_301(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 14;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([48, 120, 67, 49, 99, 56, 102, 69, 55, 102, 68, 55, 57, 53].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 14);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_302(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 17;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([48, 120, 102, 51, 50, 53, 98, 54, 57, 66, 49, 53, 69, 48, 101, 54, 51].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 17);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_303(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 17;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([48, 120, 102, 51, 50, 53, 98, 54, 57, 66, 49, 53, 69, 48, 101, 54, 51].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 17);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_304(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 9;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([48, 120, 102, 66, 65, 66, 99, 97, 49].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 9);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_305(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 9;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([48, 120, 102, 66, 65, 66, 99, 97, 49].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 9);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_306(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 18;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([48, 120, 53, 102, 99, 69, 52, 53, 67, 102, 55, 97, 57, 52, 99, 97, 102, 100].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 18);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_307(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 18;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([48, 120, 53, 102, 99, 69, 52, 53, 67, 102, 55, 97, 57, 52, 99, 97, 102, 100].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 18);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_308(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 22;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([48, 120, 56, 56, 52, 69, 69, 50, 68, 56, 52, 102, 100, 67, 50, 52, 49, 99, 53, 69, 65, 101].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 22);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_309(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 22;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([48, 120, 56, 56, 52, 69, 69, 50, 68, 56, 52, 102, 100, 67, 50, 52, 49, 99, 53, 69, 65, 101].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 22);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_310(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 19;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([48, 120, 51, 102, 57, 52, 49, 51, 52, 68, 101, 52, 49, 48, 70, 54, 70, 51, 65].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 19);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_311(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 19;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([48, 120, 51, 102, 57, 52, 49, 51, 52, 68, 101, 52, 49, 48, 70, 54, 70, 51, 65].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 19);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_312(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 5;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([48, 120, 66, 100, 70].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 5);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_313(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 5;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([48, 120, 66, 100, 70].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 5);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_314(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 16;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([48, 120, 70, 101, 65, 52, 52, 66, 101, 52, 49, 100, 57, 49, 102, 52].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 16);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_315(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 16;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([48, 120, 70, 101, 65, 52, 52, 66, 101, 52, 49, 100, 57, 49, 102, 52].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 16);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_316(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 22;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([48, 120, 50, 54, 102, 52, 97, 56, 48, 66, 99, 97, 51, 98, 69, 52, 70, 50, 48, 52, 68, 70].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 22);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_317(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 22;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([48, 120, 50, 54, 102, 52, 97, 56, 48, 66, 99, 97, 51, 98, 69, 52, 70, 50, 48, 52, 68, 70].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 22);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_318(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 14;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([48, 120, 101, 69, 55, 66, 70, 55, 100, 53, 68, 53, 55, 49].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 14);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_319(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 14;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([48, 120, 101, 69, 55, 66, 70, 55, 100, 53, 68, 53, 55, 49].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 14);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_320(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 16;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([48, 120, 69, 53, 69, 100, 54, 65, 66, 69, 100, 53, 99, 48, 49, 69].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 16);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_321(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 16;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([48, 120, 69, 53, 69, 100, 54, 65, 66, 69, 100, 53, 99, 48, 49, 69].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 16);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_322(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 8;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([48, 120, 67, 68, 102, 101, 67, 49].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 8);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_323(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 8;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([48, 120, 67, 68, 102, 101, 67, 49].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 8);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_324(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 23;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([52, 56, 46, 48, 49, 51, 55, 54, 51, 48, 55, 50, 50, 49, 53, 54, 52, 55, 55, 57, 52, 52, 50].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 23);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_325(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 23;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([52, 56, 46, 48, 49, 51, 55, 54, 51, 48, 55, 50, 50, 49, 53, 54, 52, 55, 55, 57, 52, 52, 50].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 23);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_326(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 30;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([51, 57, 53, 53, 55, 54, 50, 49, 54, 55, 57, 52, 48, 57, 46, 48, 53, 52, 53, 57, 57, 49, 53, 57, 50, 54, 48, 50, 52, 53].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 30);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_327(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 30;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([51, 57, 53, 53, 55, 54, 50, 49, 54, 55, 57, 52, 48, 57, 46, 48, 53, 52, 53, 57, 57, 49, 53, 57, 50, 54, 48, 50, 52, 53].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 30);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_328(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 9;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([50, 55, 54, 53, 46, 54, 51, 49, 50].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 9);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_329(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 9;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([50, 55, 54, 53, 46, 54, 51, 49, 50].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 9);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_330(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 41;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([54, 50, 56, 51, 52, 50, 52, 48, 56, 54, 55, 48, 52, 48, 54, 48, 50, 50, 56, 48, 46, 51, 50, 48, 51, 57, 54, 52, 56, 48, 48, 51, 55, 50, 51, 53, 50, 49, 49, 57, 53].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 41);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_331(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 41;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([54, 50, 56, 51, 52, 50, 52, 48, 56, 54, 55, 48, 52, 48, 54, 48, 50, 50, 56, 48, 46, 51, 50, 48, 51, 57, 54, 52, 56, 48, 48, 51, 55, 50, 51, 53, 50, 49, 49, 57, 53].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 41);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_332(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 14;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([54, 48, 46, 50, 54, 48, 56, 53, 56, 54, 54, 57, 57, 56].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 14);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_333(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 14;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([54, 48, 46, 50, 54, 48, 56, 53, 56, 54, 54, 57, 57, 56].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 14);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_334(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 18;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([52, 57, 54, 52, 52, 48, 51, 51, 51, 54, 51, 51, 55, 51, 56, 46, 57, 57].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 18);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_335(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 18;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([52, 57, 54, 52, 52, 48, 51, 51, 51, 54, 51, 51, 55, 51, 56, 46, 57, 57].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 18);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_336(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 23;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([56, 53, 51, 46, 54, 51, 54, 53, 57, 56, 52, 51, 54, 50, 52, 54, 56, 54, 49, 50, 51, 52, 51].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 23);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_337(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 23;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([56, 53, 51, 46, 54, 51, 54, 53, 57, 56, 52, 51, 54, 50, 52, 54, 56, 54, 49, 50, 51, 52, 51].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 23);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_338(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 17;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([55, 56, 49, 57, 49, 46, 52, 55, 57, 55, 52, 50, 57, 51, 53, 54, 51].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 17);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_339(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 17;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([55, 56, 49, 57, 49, 46, 52, 55, 57, 55, 52, 50, 57, 51, 53, 54, 51].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 17);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_340(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 7;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([53, 50, 49, 56, 46, 53, 50].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 7);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_341(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 7;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([53, 50, 49, 56, 46, 53, 50].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 7);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_342(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 28;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([54, 55, 51, 56, 49, 56, 57, 51, 50, 57, 48, 54, 48, 50, 53, 57, 52, 46, 57, 48, 48, 49, 56, 48, 54, 51, 55, 52].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 28);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_343(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 28;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([54, 55, 51, 56, 49, 56, 57, 51, 50, 57, 48, 54, 48, 50, 53, 57, 52, 46, 57, 48, 48, 49, 56, 48, 54, 51, 55, 52].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 28);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_344(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 18;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([54, 53, 53, 46, 48, 51, 57, 49, 49, 52, 51, 51, 51, 54, 50, 50, 48, 50].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 18);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_345(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 18;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([54, 53, 53, 46, 48, 51, 57, 49, 49, 52, 51, 51, 51, 54, 50, 50, 48, 50].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 18);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_346(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 36;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([53, 48, 56, 54, 48, 57, 53, 55, 48, 55, 49, 48, 49, 56, 50, 48, 52, 51, 46, 52, 51, 55, 52, 53, 56, 50, 48, 57, 56, 51, 52, 48, 51, 56, 49, 54].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 36);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_347(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 36;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([53, 48, 56, 54, 48, 57, 53, 55, 48, 55, 49, 48, 49, 56, 50, 48, 52, 51, 46, 52, 51, 55, 52, 53, 56, 50, 48, 57, 56, 51, 52, 48, 51, 56, 49, 54].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 36);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_348(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 28;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([52, 54, 56, 51, 52, 54, 57, 55, 48, 50, 52, 51, 57, 49, 52, 56, 48, 49, 53, 46, 56, 53, 53, 50, 55, 54, 49, 49].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 28);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_349(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 28;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([52, 54, 56, 51, 52, 54, 57, 55, 48, 50, 52, 51, 57, 49, 52, 56, 48, 49, 53, 46, 56, 53, 53, 50, 55, 54, 49, 49].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 28);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_350(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 21;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([49, 53, 56, 48, 53, 56, 57, 54, 51, 53, 51, 53, 50, 54, 56, 54, 46, 48, 49, 50, 54].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 21);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_351(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 21;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([49, 53, 56, 48, 53, 56, 57, 54, 51, 53, 51, 53, 50, 54, 56, 54, 46, 48, 49, 50, 54].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 21);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_352(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 5;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([56, 54, 46, 48, 52].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 5);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_353(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 5;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([56, 54, 46, 48, 52].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 5);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_354(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 6;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([47, 34, 102, 111, 111, 47].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 6);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_355(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 6;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([47, 34, 102, 111, 111, 47].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 6);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_356(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 6;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([35, 108, 111, 99, 97, 108].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 6);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_357(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([32].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_358(&mut self, depth: usize) {
        if depth >= 15 { return; }
        match self.rand() % 15 {
            0 => self.fragment_54(depth + 1),
            1 => self.fragment_56(depth + 1),
            2 => self.fragment_58(depth + 1),
            3 => self.fragment_60(depth + 1),
            4 => self.fragment_62(depth + 1),
            5 => self.fragment_64(depth + 1),
            6 => self.fragment_66(depth + 1),
            7 => self.fragment_68(depth + 1),
            8 => self.fragment_70(depth + 1),
            9 => self.fragment_72(depth + 1),
            10 => self.fragment_74(depth + 1),
            11 => self.fragment_76(depth + 1),
            12 => self.fragment_78(depth + 1),
            13 => self.fragment_80(depth + 1),
            14 => self.fragment_82(depth + 1),
            _ => unreachable!(),
        }
    }
    fn fragment_359(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([32].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_360(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([40].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_361(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([32].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_362(&mut self, depth: usize) {
        if depth >= 15 { return; }
        match self.rand() % 4 {
            0 => self.fragment_455(depth + 1),
            1 => self.fragment_462(depth + 1),
            2 => self.fragment_467(depth + 1),
            3 => self.fragment_469(depth + 1),
            _ => unreachable!(),
        }
    }
    fn fragment_363(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([32].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_364(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([41].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_365(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([32].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_366(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([10].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_367(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([32].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_368(&mut self, depth: usize) {
        if depth >= 15 { return; }
        self.fragment_356(depth + 1);
        self.fragment_357(depth + 1);
        self.fragment_358(depth + 1);
        self.fragment_359(depth + 1);
        self.fragment_360(depth + 1);
        self.fragment_361(depth + 1);
        self.fragment_362(depth + 1);
        self.fragment_363(depth + 1);
        self.fragment_364(depth + 1);
        self.fragment_365(depth + 1);
        self.fragment_366(depth + 1);
        self.fragment_367(depth + 1);
    }
    fn fragment_369(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 8;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([35, 109, 101, 115, 115, 97, 103, 101].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 8);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_370(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([32].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_371(&mut self, depth: usize) {
        if depth >= 15 { return; }
        match self.rand() % 15 {
            0 => self.fragment_54(depth + 1),
            1 => self.fragment_56(depth + 1),
            2 => self.fragment_58(depth + 1),
            3 => self.fragment_60(depth + 1),
            4 => self.fragment_62(depth + 1),
            5 => self.fragment_64(depth + 1),
            6 => self.fragment_66(depth + 1),
            7 => self.fragment_68(depth + 1),
            8 => self.fragment_70(depth + 1),
            9 => self.fragment_72(depth + 1),
            10 => self.fragment_74(depth + 1),
            11 => self.fragment_76(depth + 1),
            12 => self.fragment_78(depth + 1),
            13 => self.fragment_80(depth + 1),
            14 => self.fragment_82(depth + 1),
            _ => unreachable!(),
        }
    }
    fn fragment_372(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([32].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_373(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([40].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_374(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([32].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_375(&mut self, depth: usize) {
        if depth >= 15 { return; }
        match self.rand() % 2 {
            0 => self.fragment_441(depth + 1),
            1 => self.fragment_446(depth + 1),
            _ => unreachable!(),
        }
    }
    fn fragment_376(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([32].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_377(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([41].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_378(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([32].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_379(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([10].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_380(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([32].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_381(&mut self, depth: usize) {
        if depth >= 15 { return; }
        self.fragment_369(depth + 1);
        self.fragment_370(depth + 1);
        self.fragment_371(depth + 1);
        self.fragment_372(depth + 1);
        self.fragment_373(depth + 1);
        self.fragment_374(depth + 1);
        self.fragment_375(depth + 1);
        self.fragment_376(depth + 1);
        self.fragment_377(depth + 1);
        self.fragment_378(depth + 1);
        self.fragment_379(depth + 1);
        self.fragment_380(depth + 1);
    }
    fn fragment_382(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 0;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 0);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_383(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([32].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_384(&mut self, depth: usize) {
        if depth >= 15 { return; }
        match self.rand() % 2 {
            0 => self.fragment_184(depth + 1),
            1 => self.fragment_186(depth + 1),
            _ => unreachable!(),
        }
    }
    fn fragment_385(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([32].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_386(&mut self, depth: usize) {
        if depth >= 15 { return; }
        match self.rand() % 2 {
            0 => self.fragment_110(depth + 1),
            1 => self.fragment_112(depth + 1),
            _ => unreachable!(),
        }
    }
    fn fragment_387(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([32].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_388(&mut self, depth: usize) {
        if depth >= 15 { return; }
        self.fragment_382(depth + 1);
        self.fragment_383(depth + 1);
        self.fragment_384(depth + 1);
        self.fragment_385(depth + 1);
        self.fragment_386(depth + 1);
        self.fragment_387(depth + 1);
    }
    fn fragment_389(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 2;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([86, 67].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 2);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_390(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 2;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([86, 67].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 2);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_391(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 9;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([78, 73, 98, 106, 71, 52, 107, 78, 72].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 9);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_392(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 9;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([78, 73, 98, 106, 71, 52, 107, 78, 72].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 9);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_393(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 3;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([75, 73, 52].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 3);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_394(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 3;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([75, 73, 52].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 3);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_395(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 19;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([83, 118, 120, 84, 108, 67, 88, 85, 83, 77, 102, 95, 53, 119, 102, 116, 100, 75, 77].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 19);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_396(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 19;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([83, 118, 120, 84, 108, 67, 88, 85, 83, 77, 102, 95, 53, 119, 102, 116, 100, 75, 77].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 19);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_397(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 11;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([81, 90, 100, 90, 105, 66, 56, 69, 55, 70, 82].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 11);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_398(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 11;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([81, 90, 100, 90, 105, 66, 56, 69, 55, 70, 82].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 11);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_399(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 14;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([87, 70, 80, 83, 103, 79, 65, 98, 117, 117, 71, 71, 114, 73].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 14);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_400(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 14;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([87, 70, 80, 83, 103, 79, 65, 98, 117, 117, 71, 71, 114, 73].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 14);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_401(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 2;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([84, 81].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 2);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_402(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 2;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([84, 81].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 2);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_403(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 17;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([65, 108, 101, 105, 114, 105, 113, 54, 81, 87, 102, 52, 67, 82, 82, 80, 71].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 17);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_404(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 17;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([65, 108, 101, 105, 114, 105, 113, 54, 81, 87, 102, 52, 67, 82, 82, 80, 71].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 17);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_405(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 9;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([88, 52, 67, 66, 101, 117, 104, 116, 122].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 9);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_406(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 9;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([88, 52, 67, 66, 101, 117, 104, 116, 122].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 9);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_407(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 13;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([90, 66, 52, 51, 84, 90, 97, 48, 118, 70, 100, 120, 83].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 13);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_408(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 13;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([90, 66, 52, 51, 84, 90, 97, 48, 118, 70, 100, 120, 83].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 13);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_409(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 14;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([89, 87, 67, 88, 49, 109, 122, 119, 65, 98, 102, 86, 51, 72].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 14);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_410(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 14;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([89, 87, 67, 88, 49, 109, 122, 119, 65, 98, 102, 86, 51, 72].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 14);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_411(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 18;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([73, 107, 101, 73, 111, 50, 103, 77, 97, 72, 79, 71, 51, 117, 106, 50, 95, 78].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 18);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_412(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 18;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([73, 107, 101, 73, 111, 50, 103, 77, 97, 72, 79, 71, 51, 117, 106, 50, 95, 78].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 18);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_413(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 11;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([77, 84, 85, 88, 54, 80, 106, 78, 77, 106, 97].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 11);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_414(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 11;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([77, 84, 85, 88, 54, 80, 106, 78, 77, 106, 97].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 11);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_415(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 5;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([66, 73, 117, 71, 119].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 5);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_416(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 5;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([66, 73, 117, 71, 119].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 5);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_417(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 20;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([79, 67, 68, 69, 78, 65, 109, 101, 119, 102, 80, 118, 76, 104, 82, 95, 100, 106, 85, 106].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 20);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_418(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 20;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([79, 67, 68, 69, 78, 65, 109, 101, 119, 102, 80, 118, 76, 104, 82, 95, 100, 106, 85, 106].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 20);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_419(&mut self, depth: usize) {
        if depth >= 15 { return; }
        match self.rand() % 15 {
            0 => self.fragment_390(depth + 1),
            1 => self.fragment_392(depth + 1),
            2 => self.fragment_394(depth + 1),
            3 => self.fragment_396(depth + 1),
            4 => self.fragment_398(depth + 1),
            5 => self.fragment_400(depth + 1),
            6 => self.fragment_402(depth + 1),
            7 => self.fragment_404(depth + 1),
            8 => self.fragment_406(depth + 1),
            9 => self.fragment_408(depth + 1),
            10 => self.fragment_410(depth + 1),
            11 => self.fragment_412(depth + 1),
            12 => self.fragment_414(depth + 1),
            13 => self.fragment_416(depth + 1),
            14 => self.fragment_418(depth + 1),
            _ => unreachable!(),
        }
    }
    fn fragment_420(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([32].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_421(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([44].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_422(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([32].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_423(&mut self, depth: usize) {
        if depth >= 15 { return; }
        match self.rand() % 2 {
            0 => self.fragment_425(depth + 1),
            1 => self.fragment_427(depth + 1),
            _ => unreachable!(),
        }
    }
    fn fragment_424(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([32].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_425(&mut self, depth: usize) {
        if depth >= 15 { return; }
        self.fragment_419(depth + 1);
        self.fragment_420(depth + 1);
        self.fragment_421(depth + 1);
        self.fragment_422(depth + 1);
        self.fragment_423(depth + 1);
        self.fragment_424(depth + 1);
    }
    fn fragment_426(&mut self, depth: usize) {
        if depth >= 15 { return; }
        match self.rand() % 15 {
            0 => self.fragment_390(depth + 1),
            1 => self.fragment_392(depth + 1),
            2 => self.fragment_394(depth + 1),
            3 => self.fragment_396(depth + 1),
            4 => self.fragment_398(depth + 1),
            5 => self.fragment_400(depth + 1),
            6 => self.fragment_402(depth + 1),
            7 => self.fragment_404(depth + 1),
            8 => self.fragment_406(depth + 1),
            9 => self.fragment_408(depth + 1),
            10 => self.fragment_410(depth + 1),
            11 => self.fragment_412(depth + 1),
            12 => self.fragment_414(depth + 1),
            13 => self.fragment_416(depth + 1),
            14 => self.fragment_418(depth + 1),
            _ => unreachable!(),
        }
    }
    fn fragment_427(&mut self, depth: usize) {
        if depth >= 15 { return; }
        match self.rand() % 15 {
            0 => self.fragment_390(depth + 1),
            1 => self.fragment_392(depth + 1),
            2 => self.fragment_394(depth + 1),
            3 => self.fragment_396(depth + 1),
            4 => self.fragment_398(depth + 1),
            5 => self.fragment_400(depth + 1),
            6 => self.fragment_402(depth + 1),
            7 => self.fragment_404(depth + 1),
            8 => self.fragment_406(depth + 1),
            9 => self.fragment_408(depth + 1),
            10 => self.fragment_410(depth + 1),
            11 => self.fragment_412(depth + 1),
            12 => self.fragment_414(depth + 1),
            13 => self.fragment_416(depth + 1),
            14 => self.fragment_418(depth + 1),
            _ => unreachable!(),
        }
    }
    fn fragment_428(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([33].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_429(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([32].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_430(&mut self, depth: usize) {
        if depth >= 15 { return; }
        self.fragment_488(depth + 1);
        self.fragment_489(depth + 1);
        self.fragment_490(depth + 1);
        self.fragment_491(depth + 1);
        self.fragment_492(depth + 1);
        self.fragment_493(depth + 1);
        self.fragment_494(depth + 1);
        self.fragment_495(depth + 1);
    }
    fn fragment_431(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([32].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_432(&mut self, depth: usize) {
        if depth >= 15 { return; }
        self.fragment_428(depth + 1);
        self.fragment_429(depth + 1);
        self.fragment_430(depth + 1);
        self.fragment_431(depth + 1);
    }
    fn fragment_433(&mut self, depth: usize) {
        if depth >= 15 { return; }
        match self.rand() % 11 {
            0 => self.fragment_500(depth + 1),
            1 => self.fragment_502(depth + 1),
            2 => self.fragment_504(depth + 1),
            3 => self.fragment_506(depth + 1),
            4 => self.fragment_508(depth + 1),
            5 => self.fragment_510(depth + 1),
            6 => self.fragment_512(depth + 1),
            7 => self.fragment_514(depth + 1),
            8 => self.fragment_516(depth + 1),
            9 => self.fragment_518(depth + 1),
            10 => self.fragment_520(depth + 1),
            _ => unreachable!(),
        }
    }
    fn fragment_434(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([32].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_435(&mut self, depth: usize) {
        if depth >= 15 { return; }
        match self.rand() % 15 {
            0 => self.fragment_390(depth + 1),
            1 => self.fragment_392(depth + 1),
            2 => self.fragment_394(depth + 1),
            3 => self.fragment_396(depth + 1),
            4 => self.fragment_398(depth + 1),
            5 => self.fragment_400(depth + 1),
            6 => self.fragment_402(depth + 1),
            7 => self.fragment_404(depth + 1),
            8 => self.fragment_406(depth + 1),
            9 => self.fragment_408(depth + 1),
            10 => self.fragment_410(depth + 1),
            11 => self.fragment_412(depth + 1),
            12 => self.fragment_414(depth + 1),
            13 => self.fragment_416(depth + 1),
            14 => self.fragment_418(depth + 1),
            _ => unreachable!(),
        }
    }
    fn fragment_436(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([32].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_437(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([44].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_438(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([32].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_439(&mut self, depth: usize) {
        if depth >= 15 { return; }
        match self.rand() % 2 {
            0 => self.fragment_441(depth + 1),
            1 => self.fragment_446(depth + 1),
            _ => unreachable!(),
        }
    }
    fn fragment_440(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([32].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_441(&mut self, depth: usize) {
        if depth >= 15 { return; }
        self.fragment_433(depth + 1);
        self.fragment_434(depth + 1);
        self.fragment_435(depth + 1);
        self.fragment_436(depth + 1);
        self.fragment_437(depth + 1);
        self.fragment_438(depth + 1);
        self.fragment_439(depth + 1);
        self.fragment_440(depth + 1);
    }
    fn fragment_442(&mut self, depth: usize) {
        if depth >= 15 { return; }
        match self.rand() % 11 {
            0 => self.fragment_500(depth + 1),
            1 => self.fragment_502(depth + 1),
            2 => self.fragment_504(depth + 1),
            3 => self.fragment_506(depth + 1),
            4 => self.fragment_508(depth + 1),
            5 => self.fragment_510(depth + 1),
            6 => self.fragment_512(depth + 1),
            7 => self.fragment_514(depth + 1),
            8 => self.fragment_516(depth + 1),
            9 => self.fragment_518(depth + 1),
            10 => self.fragment_520(depth + 1),
            _ => unreachable!(),
        }
    }
    fn fragment_443(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([32].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_444(&mut self, depth: usize) {
        if depth >= 15 { return; }
        match self.rand() % 15 {
            0 => self.fragment_390(depth + 1),
            1 => self.fragment_392(depth + 1),
            2 => self.fragment_394(depth + 1),
            3 => self.fragment_396(depth + 1),
            4 => self.fragment_398(depth + 1),
            5 => self.fragment_400(depth + 1),
            6 => self.fragment_402(depth + 1),
            7 => self.fragment_404(depth + 1),
            8 => self.fragment_406(depth + 1),
            9 => self.fragment_408(depth + 1),
            10 => self.fragment_410(depth + 1),
            11 => self.fragment_412(depth + 1),
            12 => self.fragment_414(depth + 1),
            13 => self.fragment_416(depth + 1),
            14 => self.fragment_418(depth + 1),
            _ => unreachable!(),
        }
    }
    fn fragment_445(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([32].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_446(&mut self, depth: usize) {
        if depth >= 15 { return; }
        self.fragment_442(depth + 1);
        self.fragment_443(depth + 1);
        self.fragment_444(depth + 1);
        self.fragment_445(depth + 1);
    }
    fn fragment_447(&mut self, depth: usize) {
        if depth >= 15 { return; }
        match self.rand() % 11 {
            0 => self.fragment_500(depth + 1),
            1 => self.fragment_502(depth + 1),
            2 => self.fragment_504(depth + 1),
            3 => self.fragment_506(depth + 1),
            4 => self.fragment_508(depth + 1),
            5 => self.fragment_510(depth + 1),
            6 => self.fragment_512(depth + 1),
            7 => self.fragment_514(depth + 1),
            8 => self.fragment_516(depth + 1),
            9 => self.fragment_518(depth + 1),
            10 => self.fragment_520(depth + 1),
            _ => unreachable!(),
        }
    }
    fn fragment_448(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([32].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_449(&mut self, depth: usize) {
        if depth >= 15 { return; }
        match self.rand() % 15 {
            0 => self.fragment_390(depth + 1),
            1 => self.fragment_392(depth + 1),
            2 => self.fragment_394(depth + 1),
            3 => self.fragment_396(depth + 1),
            4 => self.fragment_398(depth + 1),
            5 => self.fragment_400(depth + 1),
            6 => self.fragment_402(depth + 1),
            7 => self.fragment_404(depth + 1),
            8 => self.fragment_406(depth + 1),
            9 => self.fragment_408(depth + 1),
            10 => self.fragment_410(depth + 1),
            11 => self.fragment_412(depth + 1),
            12 => self.fragment_414(depth + 1),
            13 => self.fragment_416(depth + 1),
            14 => self.fragment_418(depth + 1),
            _ => unreachable!(),
        }
    }
    fn fragment_450(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([32].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_451(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([44].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_452(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([32].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_453(&mut self, depth: usize) {
        if depth >= 15 { return; }
        match self.rand() % 4 {
            0 => self.fragment_455(depth + 1),
            1 => self.fragment_462(depth + 1),
            2 => self.fragment_467(depth + 1),
            3 => self.fragment_469(depth + 1),
            _ => unreachable!(),
        }
    }
    fn fragment_454(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([32].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_455(&mut self, depth: usize) {
        if depth >= 15 { return; }
        self.fragment_447(depth + 1);
        self.fragment_448(depth + 1);
        self.fragment_449(depth + 1);
        self.fragment_450(depth + 1);
        self.fragment_451(depth + 1);
        self.fragment_452(depth + 1);
        self.fragment_453(depth + 1);
        self.fragment_454(depth + 1);
    }
    fn fragment_456(&mut self, depth: usize) {
        if depth >= 15 { return; }
        match self.rand() % 15 {
            0 => self.fragment_390(depth + 1),
            1 => self.fragment_392(depth + 1),
            2 => self.fragment_394(depth + 1),
            3 => self.fragment_396(depth + 1),
            4 => self.fragment_398(depth + 1),
            5 => self.fragment_400(depth + 1),
            6 => self.fragment_402(depth + 1),
            7 => self.fragment_404(depth + 1),
            8 => self.fragment_406(depth + 1),
            9 => self.fragment_408(depth + 1),
            10 => self.fragment_410(depth + 1),
            11 => self.fragment_412(depth + 1),
            12 => self.fragment_414(depth + 1),
            13 => self.fragment_416(depth + 1),
            14 => self.fragment_418(depth + 1),
            _ => unreachable!(),
        }
    }
    fn fragment_457(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([32].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_458(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([44].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_459(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([32].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_460(&mut self, depth: usize) {
        if depth >= 15 { return; }
        match self.rand() % 4 {
            0 => self.fragment_455(depth + 1),
            1 => self.fragment_462(depth + 1),
            2 => self.fragment_467(depth + 1),
            3 => self.fragment_469(depth + 1),
            _ => unreachable!(),
        }
    }
    fn fragment_461(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([32].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_462(&mut self, depth: usize) {
        if depth >= 15 { return; }
        self.fragment_456(depth + 1);
        self.fragment_457(depth + 1);
        self.fragment_458(depth + 1);
        self.fragment_459(depth + 1);
        self.fragment_460(depth + 1);
        self.fragment_461(depth + 1);
    }
    fn fragment_463(&mut self, depth: usize) {
        if depth >= 15 { return; }
        match self.rand() % 11 {
            0 => self.fragment_500(depth + 1),
            1 => self.fragment_502(depth + 1),
            2 => self.fragment_504(depth + 1),
            3 => self.fragment_506(depth + 1),
            4 => self.fragment_508(depth + 1),
            5 => self.fragment_510(depth + 1),
            6 => self.fragment_512(depth + 1),
            7 => self.fragment_514(depth + 1),
            8 => self.fragment_516(depth + 1),
            9 => self.fragment_518(depth + 1),
            10 => self.fragment_520(depth + 1),
            _ => unreachable!(),
        }
    }
    fn fragment_464(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([32].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_465(&mut self, depth: usize) {
        if depth >= 15 { return; }
        match self.rand() % 15 {
            0 => self.fragment_390(depth + 1),
            1 => self.fragment_392(depth + 1),
            2 => self.fragment_394(depth + 1),
            3 => self.fragment_396(depth + 1),
            4 => self.fragment_398(depth + 1),
            5 => self.fragment_400(depth + 1),
            6 => self.fragment_402(depth + 1),
            7 => self.fragment_404(depth + 1),
            8 => self.fragment_406(depth + 1),
            9 => self.fragment_408(depth + 1),
            10 => self.fragment_410(depth + 1),
            11 => self.fragment_412(depth + 1),
            12 => self.fragment_414(depth + 1),
            13 => self.fragment_416(depth + 1),
            14 => self.fragment_418(depth + 1),
            _ => unreachable!(),
        }
    }
    fn fragment_466(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([32].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_467(&mut self, depth: usize) {
        if depth >= 15 { return; }
        self.fragment_463(depth + 1);
        self.fragment_464(depth + 1);
        self.fragment_465(depth + 1);
        self.fragment_466(depth + 1);
    }
    fn fragment_468(&mut self, depth: usize) {
        if depth >= 15 { return; }
        match self.rand() % 15 {
            0 => self.fragment_390(depth + 1),
            1 => self.fragment_392(depth + 1),
            2 => self.fragment_394(depth + 1),
            3 => self.fragment_396(depth + 1),
            4 => self.fragment_398(depth + 1),
            5 => self.fragment_400(depth + 1),
            6 => self.fragment_402(depth + 1),
            7 => self.fragment_404(depth + 1),
            8 => self.fragment_406(depth + 1),
            9 => self.fragment_408(depth + 1),
            10 => self.fragment_410(depth + 1),
            11 => self.fragment_412(depth + 1),
            12 => self.fragment_414(depth + 1),
            13 => self.fragment_416(depth + 1),
            14 => self.fragment_418(depth + 1),
            _ => unreachable!(),
        }
    }
    fn fragment_469(&mut self, depth: usize) {
        if depth >= 15 { return; }
        match self.rand() % 15 {
            0 => self.fragment_390(depth + 1),
            1 => self.fragment_392(depth + 1),
            2 => self.fragment_394(depth + 1),
            3 => self.fragment_396(depth + 1),
            4 => self.fragment_398(depth + 1),
            5 => self.fragment_400(depth + 1),
            6 => self.fragment_402(depth + 1),
            7 => self.fragment_404(depth + 1),
            8 => self.fragment_406(depth + 1),
            9 => self.fragment_408(depth + 1),
            10 => self.fragment_410(depth + 1),
            11 => self.fragment_412(depth + 1),
            12 => self.fragment_414(depth + 1),
            13 => self.fragment_416(depth + 1),
            14 => self.fragment_418(depth + 1),
            _ => unreachable!(),
        }
    }
    fn fragment_470(&mut self, depth: usize) {
        if depth >= 15 { return; }
        match self.rand() % 4 {
            0 => self.fragment_84(depth + 1),
            1 => self.fragment_86(depth + 1),
            2 => self.fragment_88(depth + 1),
            3 => self.fragment_90(depth + 1),
            _ => unreachable!(),
        }
    }
    fn fragment_471(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([32].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_472(&mut self, depth: usize) {
        if depth >= 15 { return; }
        match self.rand() % 11 {
            0 => self.fragment_500(depth + 1),
            1 => self.fragment_502(depth + 1),
            2 => self.fragment_504(depth + 1),
            3 => self.fragment_506(depth + 1),
            4 => self.fragment_508(depth + 1),
            5 => self.fragment_510(depth + 1),
            6 => self.fragment_512(depth + 1),
            7 => self.fragment_514(depth + 1),
            8 => self.fragment_516(depth + 1),
            9 => self.fragment_518(depth + 1),
            10 => self.fragment_520(depth + 1),
            _ => unreachable!(),
        }
    }
    fn fragment_473(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([32].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_474(&mut self, depth: usize) {
        if depth >= 15 { return; }
        match self.rand() % 15 {
            0 => self.fragment_390(depth + 1),
            1 => self.fragment_392(depth + 1),
            2 => self.fragment_394(depth + 1),
            3 => self.fragment_396(depth + 1),
            4 => self.fragment_398(depth + 1),
            5 => self.fragment_400(depth + 1),
            6 => self.fragment_402(depth + 1),
            7 => self.fragment_404(depth + 1),
            8 => self.fragment_406(depth + 1),
            9 => self.fragment_408(depth + 1),
            10 => self.fragment_410(depth + 1),
            11 => self.fragment_412(depth + 1),
            12 => self.fragment_414(depth + 1),
            13 => self.fragment_416(depth + 1),
            14 => self.fragment_418(depth + 1),
            _ => unreachable!(),
        }
    }
    fn fragment_475(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([32].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_476(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([44].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_477(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([32].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_478(&mut self, depth: usize) {
        if depth >= 15 { return; }
        match self.rand() % 2 {
            0 => self.fragment_480(depth + 1),
            1 => self.fragment_487(depth + 1),
            _ => unreachable!(),
        }
    }
    fn fragment_479(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([32].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_480(&mut self, depth: usize) {
        if depth >= 15 { return; }
        self.fragment_470(depth + 1);
        self.fragment_471(depth + 1);
        self.fragment_472(depth + 1);
        self.fragment_473(depth + 1);
        self.fragment_474(depth + 1);
        self.fragment_475(depth + 1);
        self.fragment_476(depth + 1);
        self.fragment_477(depth + 1);
        self.fragment_478(depth + 1);
        self.fragment_479(depth + 1);
    }
    fn fragment_481(&mut self, depth: usize) {
        if depth >= 15 { return; }
        match self.rand() % 4 {
            0 => self.fragment_84(depth + 1),
            1 => self.fragment_86(depth + 1),
            2 => self.fragment_88(depth + 1),
            3 => self.fragment_90(depth + 1),
            _ => unreachable!(),
        }
    }
    fn fragment_482(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([32].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_483(&mut self, depth: usize) {
        if depth >= 15 { return; }
        match self.rand() % 11 {
            0 => self.fragment_500(depth + 1),
            1 => self.fragment_502(depth + 1),
            2 => self.fragment_504(depth + 1),
            3 => self.fragment_506(depth + 1),
            4 => self.fragment_508(depth + 1),
            5 => self.fragment_510(depth + 1),
            6 => self.fragment_512(depth + 1),
            7 => self.fragment_514(depth + 1),
            8 => self.fragment_516(depth + 1),
            9 => self.fragment_518(depth + 1),
            10 => self.fragment_520(depth + 1),
            _ => unreachable!(),
        }
    }
    fn fragment_484(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([32].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_485(&mut self, depth: usize) {
        if depth >= 15 { return; }
        match self.rand() % 15 {
            0 => self.fragment_390(depth + 1),
            1 => self.fragment_392(depth + 1),
            2 => self.fragment_394(depth + 1),
            3 => self.fragment_396(depth + 1),
            4 => self.fragment_398(depth + 1),
            5 => self.fragment_400(depth + 1),
            6 => self.fragment_402(depth + 1),
            7 => self.fragment_404(depth + 1),
            8 => self.fragment_406(depth + 1),
            9 => self.fragment_408(depth + 1),
            10 => self.fragment_410(depth + 1),
            11 => self.fragment_412(depth + 1),
            12 => self.fragment_414(depth + 1),
            13 => self.fragment_416(depth + 1),
            14 => self.fragment_418(depth + 1),
            _ => unreachable!(),
        }
    }
    fn fragment_486(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([32].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_487(&mut self, depth: usize) {
        if depth >= 15 { return; }
        self.fragment_481(depth + 1);
        self.fragment_482(depth + 1);
        self.fragment_483(depth + 1);
        self.fragment_484(depth + 1);
        self.fragment_485(depth + 1);
        self.fragment_486(depth + 1);
    }
    fn fragment_488(&mut self, depth: usize) {
        if depth >= 15 { return; }
        match self.rand() % 15 {
            0 => self.fragment_54(depth + 1),
            1 => self.fragment_56(depth + 1),
            2 => self.fragment_58(depth + 1),
            3 => self.fragment_60(depth + 1),
            4 => self.fragment_62(depth + 1),
            5 => self.fragment_64(depth + 1),
            6 => self.fragment_66(depth + 1),
            7 => self.fragment_68(depth + 1),
            8 => self.fragment_70(depth + 1),
            9 => self.fragment_72(depth + 1),
            10 => self.fragment_74(depth + 1),
            11 => self.fragment_76(depth + 1),
            12 => self.fragment_78(depth + 1),
            13 => self.fragment_80(depth + 1),
            14 => self.fragment_82(depth + 1),
            _ => unreachable!(),
        }
    }
    fn fragment_489(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([32].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_490(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([40].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_491(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([32].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_492(&mut self, depth: usize) {
        if depth >= 15 { return; }
        match self.rand() % 2 {
            0 => self.fragment_50(depth + 1),
            1 => self.fragment_52(depth + 1),
            _ => unreachable!(),
        }
    }
    fn fragment_493(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([32].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_494(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([41].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_495(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([32].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_496(&mut self, depth: usize) {
        if depth >= 15 { return; }
        self.fragment_488(depth + 1);
        self.fragment_489(depth + 1);
        self.fragment_490(depth + 1);
        self.fragment_491(depth + 1);
        self.fragment_492(depth + 1);
        self.fragment_493(depth + 1);
        self.fragment_494(depth + 1);
        self.fragment_495(depth + 1);
    }
    fn fragment_497(&mut self, depth: usize) {
        if depth >= 15 { return; }
        self.fragment_382(depth + 1);
        self.fragment_383(depth + 1);
        self.fragment_384(depth + 1);
        self.fragment_385(depth + 1);
        self.fragment_386(depth + 1);
        self.fragment_387(depth + 1);
    }
    fn fragment_498(&mut self, depth: usize) {
        if depth >= 15 { return; }
        self.fragment_382(depth + 1);
        self.fragment_383(depth + 1);
        self.fragment_384(depth + 1);
        self.fragment_385(depth + 1);
        self.fragment_386(depth + 1);
        self.fragment_387(depth + 1);
    }
    fn fragment_499(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 3;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([64, 105, 56].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 3);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_500(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 3;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([64, 105, 56].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 3);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_501(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 4;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([64, 105, 49, 54].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 4);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_502(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 4;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([64, 105, 49, 54].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 4);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_503(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 4;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([64, 105, 51, 50].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 4);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_504(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 4;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([64, 105, 51, 50].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 4);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_505(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 4;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([64, 105, 54, 52].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 4);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_506(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 4;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([64, 105, 54, 52].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 4);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_507(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 3;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([64, 117, 56].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 3);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_508(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 3;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([64, 117, 56].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 3);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_509(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 4;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([64, 117, 49, 54].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 4);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_510(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 4;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([64, 117, 49, 54].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 4);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_511(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 4;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([64, 117, 51, 50].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 4);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_512(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 4;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([64, 117, 51, 50].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 4);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_513(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 4;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([64, 117, 54, 52].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 4);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_514(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 4;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([64, 117, 54, 52].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 4);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_515(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 4;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([64, 102, 51, 50].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 4);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_516(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 4;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([64, 102, 51, 50].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 4);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_517(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 4;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([64, 102, 54, 52].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 4);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_518(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 4;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([64, 102, 54, 52].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 4);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_519(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 4;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([64, 115, 116, 114].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 4);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_520(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 4;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([64, 115, 116, 114].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 4);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_521(&mut self, depth: usize) {
        if depth >= 15 { return; }
        match self.rand() % 15 {
            0 => self.fragment_390(depth + 1),
            1 => self.fragment_392(depth + 1),
            2 => self.fragment_394(depth + 1),
            3 => self.fragment_396(depth + 1),
            4 => self.fragment_398(depth + 1),
            5 => self.fragment_400(depth + 1),
            6 => self.fragment_402(depth + 1),
            7 => self.fragment_404(depth + 1),
            8 => self.fragment_406(depth + 1),
            9 => self.fragment_408(depth + 1),
            10 => self.fragment_410(depth + 1),
            11 => self.fragment_412(depth + 1),
            12 => self.fragment_414(depth + 1),
            13 => self.fragment_416(depth + 1),
            14 => self.fragment_418(depth + 1),
            _ => unreachable!(),
        }
    }
    fn fragment_522(&mut self, depth: usize) {
        if depth >= 15 { return; }
        match self.rand() % 15 {
            0 => self.fragment_390(depth + 1),
            1 => self.fragment_392(depth + 1),
            2 => self.fragment_394(depth + 1),
            3 => self.fragment_396(depth + 1),
            4 => self.fragment_398(depth + 1),
            5 => self.fragment_400(depth + 1),
            6 => self.fragment_402(depth + 1),
            7 => self.fragment_404(depth + 1),
            8 => self.fragment_406(depth + 1),
            9 => self.fragment_408(depth + 1),
            10 => self.fragment_410(depth + 1),
            11 => self.fragment_412(depth + 1),
            12 => self.fragment_414(depth + 1),
            13 => self.fragment_416(depth + 1),
            14 => self.fragment_418(depth + 1),
            _ => unreachable!(),
        }
    }
    fn fragment_523(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([95].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_524(&mut self, depth: usize) {
        if depth >= 15 { return; }

            unsafe {
                let old_size = self.buf.len();
                let new_size = old_size + 1;

                if new_size > self.buf.capacity() {
                    self.buf.reserve(new_size - old_size);
                }

                std::ptr::copy_nonoverlapping([95].as_ptr(), self.buf.as_mut_ptr().offset(old_size as isize), 1);
                self.buf.set_len(new_size);
            }
        }
    fn fragment_525(&mut self, depth: usize) {
        if depth >= 15 { return; }
        match self.rand() % 2 {
            0 => self.fragment_522(depth + 1),
            1 => self.fragment_524(depth + 1),
            _ => unreachable!(),
        }
    }
    fn fragment_526(&mut self, depth: usize) {
        if depth >= 15 { return; }
        match self.rand() % 2 {
            0 => self.fragment_522(depth + 1),
            1 => self.fragment_524(depth + 1),
            _ => unreachable!(),
        }
    }
    fn fragment_527(&mut self, depth: usize) {
        if depth >= 15 { return; }
        match self.rand() % 62 {
            0 => self.fragment_233(depth + 1),
            1 => self.fragment_235(depth + 1),
            2 => self.fragment_237(depth + 1),
            3 => self.fragment_239(depth + 1),
            4 => self.fragment_241(depth + 1),
            5 => self.fragment_243(depth + 1),
            6 => self.fragment_245(depth + 1),
            7 => self.fragment_247(depth + 1),
            8 => self.fragment_249(depth + 1),
            9 => self.fragment_251(depth + 1),
            10 => self.fragment_253(depth + 1),
            11 => self.fragment_255(depth + 1),
            12 => self.fragment_257(depth + 1),
            13 => self.fragment_259(depth + 1),
            14 => self.fragment_261(depth + 1),
            15 => self.fragment_263(depth + 1),
            16 => self.fragment_265(depth + 1),
            17 => self.fragment_267(depth + 1),
            18 => self.fragment_269(depth + 1),
            19 => self.fragment_271(depth + 1),
            20 => self.fragment_273(depth + 1),
            21 => self.fragment_275(depth + 1),
            22 => self.fragment_277(depth + 1),
            23 => self.fragment_279(depth + 1),
            24 => self.fragment_281(depth + 1),
            25 => self.fragment_283(depth + 1),
            26 => self.fragment_285(depth + 1),
            27 => self.fragment_287(depth + 1),
            28 => self.fragment_289(depth + 1),
            29 => self.fragment_291(depth + 1),
            30 => self.fragment_293(depth + 1),
            31 => self.fragment_295(depth + 1),
            32 => self.fragment_297(depth + 1),
            33 => self.fragment_299(depth + 1),
            34 => self.fragment_301(depth + 1),
            35 => self.fragment_303(depth + 1),
            36 => self.fragment_305(depth + 1),
            37 => self.fragment_307(depth + 1),
            38 => self.fragment_309(depth + 1),
            39 => self.fragment_311(depth + 1),
            40 => self.fragment_313(depth + 1),
            41 => self.fragment_315(depth + 1),
            42 => self.fragment_317(depth + 1),
            43 => self.fragment_319(depth + 1),
            44 => self.fragment_321(depth + 1),
            45 => self.fragment_323(depth + 1),
            46 => self.fragment_325(depth + 1),
            47 => self.fragment_327(depth + 1),
            48 => self.fragment_329(depth + 1),
            49 => self.fragment_331(depth + 1),
            50 => self.fragment_333(depth + 1),
            51 => self.fragment_335(depth + 1),
            52 => self.fragment_337(depth + 1),
            53 => self.fragment_339(depth + 1),
            54 => self.fragment_341(depth + 1),
            55 => self.fragment_343(depth + 1),
            56 => self.fragment_345(depth + 1),
            57 => self.fragment_347(depth + 1),
            58 => self.fragment_349(depth + 1),
            59 => self.fragment_351(depth + 1),
            60 => self.fragment_353(depth + 1),
            61 => self.fragment_355(depth + 1),
            _ => unreachable!(),
        }
    }
    fn fragment_528(&mut self, depth: usize) {
        if depth >= 15 { return; }
        match self.rand() % 62 {
            0 => self.fragment_233(depth + 1),
            1 => self.fragment_235(depth + 1),
            2 => self.fragment_237(depth + 1),
            3 => self.fragment_239(depth + 1),
            4 => self.fragment_241(depth + 1),
            5 => self.fragment_243(depth + 1),
            6 => self.fragment_245(depth + 1),
            7 => self.fragment_247(depth + 1),
            8 => self.fragment_249(depth + 1),
            9 => self.fragment_251(depth + 1),
            10 => self.fragment_253(depth + 1),
            11 => self.fragment_255(depth + 1),
            12 => self.fragment_257(depth + 1),
            13 => self.fragment_259(depth + 1),
            14 => self.fragment_261(depth + 1),
            15 => self.fragment_263(depth + 1),
            16 => self.fragment_265(depth + 1),
            17 => self.fragment_267(depth + 1),
            18 => self.fragment_269(depth + 1),
            19 => self.fragment_271(depth + 1),
            20 => self.fragment_273(depth + 1),
            21 => self.fragment_275(depth + 1),
            22 => self.fragment_277(depth + 1),
            23 => self.fragment_279(depth + 1),
            24 => self.fragment_281(depth + 1),
            25 => self.fragment_283(depth + 1),
            26 => self.fragment_285(depth + 1),
            27 => self.fragment_287(depth + 1),
            28 => self.fragment_289(depth + 1),
            29 => self.fragment_291(depth + 1),
            30 => self.fragment_293(depth + 1),
            31 => self.fragment_295(depth + 1),
            32 => self.fragment_297(depth + 1),
            33 => self.fragment_299(depth + 1),
            34 => self.fragment_301(depth + 1),
            35 => self.fragment_303(depth + 1),
            36 => self.fragment_305(depth + 1),
            37 => self.fragment_307(depth + 1),
            38 => self.fragment_309(depth + 1),
            39 => self.fragment_311(depth + 1),
            40 => self.fragment_313(depth + 1),
            41 => self.fragment_315(depth + 1),
            42 => self.fragment_317(depth + 1),
            43 => self.fragment_319(depth + 1),
            44 => self.fragment_321(depth + 1),
            45 => self.fragment_323(depth + 1),
            46 => self.fragment_325(depth + 1),
            47 => self.fragment_327(depth + 1),
            48 => self.fragment_329(depth + 1),
            49 => self.fragment_331(depth + 1),
            50 => self.fragment_333(depth + 1),
            51 => self.fragment_335(depth + 1),
            52 => self.fragment_337(depth + 1),
            53 => self.fragment_339(depth + 1),
            54 => self.fragment_341(depth + 1),
            55 => self.fragment_343(depth + 1),
            56 => self.fragment_345(depth + 1),
            57 => self.fragment_347(depth + 1),
            58 => self.fragment_349(depth + 1),
            59 => self.fragment_351(depth + 1),
            60 => self.fragment_353(depth + 1),
            61 => self.fragment_355(depth + 1),
            _ => unreachable!(),
        }
    }
}
