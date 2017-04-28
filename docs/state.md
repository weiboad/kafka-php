<?php

    init
    REQUEST_METADATA =>  STATUS_LOOP 
    REQUEST_GETGROUP =>  STATUS_START
    REQUEST_JOINGROUP =>  STATUS_START
    REQUEST_SYNCGROUP =>  STATUS_START
    REQUEST_HEARTGROUP =>  STATUS_LOOP
    REQUEST_OFFSET =>  STATUS_LOOP
    REQUEST_FETCH =>  STATUS_LOOP
    REQUEST_FETCH_OFFSET => STATUS_LOOP
    REQUEST_COMMIT_OFFSET =>  STATUS_LOOP
    instance => empty

    run condition
    REQUEST_METADATA => (STATUS_LOOP) && !STATUS_PROCESS
        (run)  => status->STATUS_LOOP|STATUS_PROCESS
        (succ) => status->STATUS_LOOP|STATUS_FINISH
            (change) recover
        (fail) => retry
    REQUEST_GETGROUP => (STATUS_START) && (REQUEST_METADATA^STATUS_FINISH)
        (run)  => status->STATUS_PROCESS
        (succ) => status->STATUS_STOP|STATUS_FINISH
        (fail) => recover
    REQUEST_JOINGROUP => (STATUS_START) && (REQUEST_GETGROUP^STATUS_FINISH)
        (run)  => status->STATUS_PROCESS
        (succ) => status->STATUS_STOP|STATUS_FINISH
        (fail) => recover
    REQUEST_SYNCGROUP => (STATUS_START) && (REQUEST_JOINGROUP^STATUS_FINISH)
        (run)  => status->STATUS_PROCESS
        (succ) => status->STATUS_STOP|STATUS_FINISH
        (fail) => recover
    REQUEST_HEARTGROUP => (STATUS_LOOP) && (REQUEST_SYNCGROUP^STATUS_FINISH)
        (run)  => status->STATUS_PROCESS
        (succ) => status->STATUS_LOOP|STATUS_FINISH
        (fail) => 
                27 => if !REQUEST_JOINGROUP^STATUS_PROCESS -> modify(REQUEST_JOINGROUP init)
                25 => if !REQUEST_JOINGROUP^STATUS_PROCESS -> modify(REQUEST_JOINGROUP init) empty member_id
    
    REQUEST_OFFSET => (STATUS_LOOP) && (REQUEST_METADATA^STATUS_FINISH)
        (run)  => status->STATUS_PROCESS
        (succ) => status->STATUS_LOOP|STATUS_FINISH 
        (fail) => recover

    REQUEST_FETCH_OFFSET => (STATUS_LOOP) && (REQUEST_SYNCGROUP^STATUS_FINISH)
        (run)  => status->STATUS_PROCESS
        (succ) => status->STATUS_LOOP|STATUS_FINISH 
        (fail) => recover

    REQUEST_FETCH => (STATUS_LOOP) && (REQUEST_FETCH_OFFSET^STATUS_FINISH)
        (run)  => status->STATUS_PROCESS
        (succ) => status->STATUS_LOOP|STATUS_FINISH 
        (fail) => recover

    REQUEST_COMMIT_OFFSET => (STATUS_LOOP) && (REQUEST_FETCH^STATUS_FINISH)
        (run)  => status->STATUS_PROCESS
        (succ) => status->STATUS_LOOP|STATUS_FINISH 
        (fail) => recover

