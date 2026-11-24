;; Emscripten-style longjmp through env.invoke_vii / env.throw_longjmp. $deep
;; recurses $n frames, grows memory, then throws; the host's invoke_vii catches
;; it. wp_depth must be back to 0 although no frame ran its exit, and the store
;; into the page grown inside the nested call must land (interpreter fix).
;; RUN: main 50
;; RUN: main 1000
(module
  (import "env" "invoke_vii" (func $invoke_vii (param i32 i32 i32)))
  (import "env" "throw_longjmp" (func $throw))
  (type $vii (func (param i32 i32)))
  (memory 1 16)
  (table 1 funcref)
  (elem (i32.const 0) $deep)
  (func $deep (type $vii) (param $n i32) (param $x i32)
    (if (i32.eqz (local.get $n))
      (then
        (drop (memory.grow (i32.const 1)))
        (call $throw))
      (else (call $deep (i32.sub (local.get $n) (i32.const 1)) (local.get $x)))))
  (func (export "main") (param $n i32) (result i32)
    (call $invoke_vii (i32.const 0) (local.get $n) (i32.const 0))
    (i32.store (i32.sub (i32.mul (memory.size) (i32.const 65536)) (i32.const 4)) (i32.const 77))
    (i32.add (memory.size)
      (i32.load (i32.sub (i32.mul (memory.size) (i32.const 65536)) (i32.const 4))))))
