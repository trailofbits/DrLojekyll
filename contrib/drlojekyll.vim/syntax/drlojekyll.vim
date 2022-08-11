" Language:    Dr. Lojekyll
" Maintainer:  Brad Larsen <brad.larsen@trailofbits.com>
" URL:         https://github.com/trailofbits/DrLojekyll

if exists("b:current_syntax")
    finish
endif

" We need nocompatible mode in order to continue lines with backslashes.
" Original setting will be restored.
let s:cpo_save = &cpo
set cpo&vim

" TODO: strings
syn region drString matchgroup=Quote start=/"/ end=/"/ contains=drEscape,drEscapeInvalid,@Spell
hi def link drString String

syn region drInlineCode matchgroup=Quote start=/<!/ end=/!>/
syn region drInlineCode matchgroup=Quote start=/```\(python\|c++\)\?/ end=/```/
hi def link drInlineCode String

syn match drEscapeInvalid /\\./ contained
hi def link drEscapeInvalid Error

syn match drEscape /\\[aenrt0"]/ contained
hi def link drEscape Special

syn match drNumber /\<[1-9]\d*\>/
syn match drNumber /\<0[oO]\=\o\+\>/
syn match drNumber /\<0[xX]\x\+\>/
syn match drNumber /\<0[bB][01]\+\>/
hi def link drNumber Number

syn match drBoolean /\<\%(true\|false\)\>/ display
hi def link drBoolean Boolean

syn keyword drType
            \ f32
            \ f64
            \ i128
            \ i16
            \ i32
            \ i64
            \ i8
            \ u128
            \ u16
            \ u32
            \ u64
            \ u8
hi def link drType Type

syn keyword drKeyword
            \ aggregate
            \ bound
            \ free
            \ impure
            \ inline
            \ mutable
            \ over
            \ summary
            \ type
            \ unordered
hi def link drKeyword Keyword

syn match drDirectiveUnknown /#\S*\>/
hi def link drDirectiveUnknown Error

syn match drDirective /#\%(export\|functor\|import\|include\|inline\|local\|message\|query\|prologue\|epilogue\|constant\)\>/
syn match drDirective /\<range\s*(\s*\(\[.+*?]\)\s*)\>/
hi def link drDirective Keyword

syn keyword drTodo TODO FIXME XXX BUG contained
hi def link drTodo Todo

syn match drComment /;.*/ contains=@Spell,drTodo
hi def link drComment Comment


let b:current_syntax = "drlojekyll"

let &cpo = s:cpo_save
unlet s:cpo_save
