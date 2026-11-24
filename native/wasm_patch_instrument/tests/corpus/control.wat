;; Control flow the pass rewrites: recursion (entry charge + depth), loops,
;; branches to the function label, returns from nested blocks, multi-value
;; results, call_indirect cycles, mutable globals and memory growth.
;; RUN: fib 25
;; RUN: sum 100000
;; RUN: nested 300 200
;; RUN: brtable 4000
;; RUN: early 1000
;; RUN: divmod 1000
;; RUN: ind 2000
;; RUN: grow 3
;; RUN: counter 5000
;; RUN: hash 200000
(module
  (type $u (func (param i32) (result i32)))
  (memory 1 64)
  (table 2 funcref)
  (elem (i32.const 0) $ind_a $ind_b)
  (global $count (mut i64) (i64.const 0))
  (global $flag (mut i32) (i32.const 7))
  (global $k i32 (i32.const 3))
  (func $fib (export "fib") (param $n i32) (result i32)
    (if (result i32) (i32.lt_u (local.get $n) (i32.const 2))
      (then (local.get $n))
      (else (i32.add (call $fib (i32.sub (local.get $n) (i32.const 1)))
                     (call $fib (i32.sub (local.get $n) (i32.const 2)))))))
  (func (export "sum") (param $n i32) (result i64) (local $acc i64)
    (block $out
      (loop $l
        (br_if $out (i32.eqz (local.get $n)))
        (local.set $acc (i64.add (local.get $acc) (i64.extend_i32_u (local.get $n))))
        (local.set $n (i32.sub (local.get $n) (i32.const 1)))
        (br $l)))
    (local.get $acc))
  (func (export "nested") (param $a i32) (param $b i32) (result i32) (local $i i32) (local $j i32) (local $s i32)
    (loop $li
      (local.set $j (i32.const 0))
      (loop $lj
        (local.set $s (i32.add (local.get $s) (i32.mul (local.get $i) (local.get $j))))
        (local.set $j (i32.add (local.get $j) (i32.const 1)))
        (br_if $lj (i32.lt_u (local.get $j) (local.get $b))))
      (local.set $i (i32.add (local.get $i) (i32.const 1)))
      (br_if $li (i32.lt_u (local.get $i) (local.get $a))))
    (local.get $s))
  ;; recursive; br_table whose every target is the function label
  (func $brtable (export "brtable") (param $n i32) (result i32)
    (if (i32.eqz (local.get $n)) (then (return (i32.const 0))))
    (i32.add (i32.const 1) (call $brtable (i32.sub (local.get $n) (i32.const 1))))
    (br_table 0 0 0 (i32.and (local.get $n) (i32.const 3))))
  (func $early (export "early") (param $n i32) (result i32)
    (block $b
      (block $c
        (br_if $c (i32.eqz (local.get $n)))
        (return (i32.add (i32.const 1) (call $early (i32.sub (local.get $n) (i32.const 1))))))
      (br 1 (i32.const 0)))
    (unreachable))
  ;; recursive, two results: the wrapper block needs a type index
  (func $divmod (export "divmod") (param $n i32) (result i32 i32)
    (if (result i32 i32) (i32.lt_u (local.get $n) (i32.const 7))
      (then (i32.const 0) (local.get $n))
      (else
        (call $divmod (i32.sub (local.get $n) (i32.const 7)))
        (local.set $n)
        (i32.add (i32.const 1))
        (local.get $n))))
  ;; mutual recursion through the table only
  (func $ind_a (type $u) (param $n i32) (result i32)
    (if (result i32) (i32.eqz (local.get $n)) (then (i32.const 0))
      (else (i32.add (i32.const 2)
        (call_indirect (type $u) (i32.sub (local.get $n) (i32.const 1)) (i32.const 1))))))
  (func $ind_b (type $u) (param $n i32) (result i32)
    (if (result i32) (i32.eqz (local.get $n)) (then (i32.const 0))
      (else (i32.add (i32.const 3)
        (call_indirect (type $u) (i32.sub (local.get $n) (i32.const 1)) (i32.const 0))))))
  (func (export "ind") (param $n i32) (result i32)
    (call_indirect (type $u) (local.get $n) (i32.const 0)))
  (func (export "grow") (param $n i32) (result i32)
    (drop (memory.grow (local.get $n)))
    (i32.store (i32.mul (memory.size) (i32.const 65535)) (i32.const 0x5a5a5a5a))
    (memory.size))
  (func (export "counter") (param $n i32) (result i64)
    (loop $l
      (global.set $count (i64.add (global.get $count) (i64.const 3)))
      (global.set $flag (i32.xor (global.get $flag) (local.get $n)))
      (local.set $n (i32.sub (local.get $n) (i32.const 1)))
      (br_if $l (local.get $n)))
    (global.get $count))
  (func (export "hash") (param $n i32) (result i64) (local $h i64) (local $i i32)
    (local.set $h (i64.const 1469598103934665603))
    (loop $l
      (i32.store8 (i32.and (local.get $i) (i32.const 0xffff)) (local.get $i))
      (local.set $h (i64.mul (i64.xor (local.get $h) (i64.extend_i32_u (local.get $i))) (i64.const 1099511628211)))
      (local.set $i (i32.add (local.get $i) (i32.const 1)))
      (br_if $l (i32.lt_u (local.get $i) (local.get $n))))
    (local.get $h)))
