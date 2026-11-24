;; Never terminates uninstrumented: only the instrumented module is run.
;; SPIN: spin 0
;; SPIN: recurse 0
;; SPIN: recurse_loop 0
(module
  (memory 1 1)
  (func (export "spin") (param i32) (result i32)
    (loop $l (br $l))
    (i32.const 0))
  (func $recurse (export "recurse") (param $n i32) (result i32)
    (i32.add (i32.const 1) (call $recurse (i32.add (local.get $n) (i32.const 1)))))
  (func $rl (export "recurse_loop") (param $n i32) (result i32)
    (loop $l
      (i32.store (i32.const 0) (i32.add (i32.load (i32.const 0)) (i32.const 1)))
      (drop (call $rl (local.get $n)))
      (br $l))
    (i32.const 0)))
