namespace FsDbsp.Lib

open System
open System.Collections.Generic

// type Numeric<'t when 't : equality
//                  and 't : comparison
//                  and 't : (static member get_Zero : Unit -> 't)
//                  and 't : (static member ( + ) : 't * 't -> 't)
//                  and 't : (static member ( - ) : 't * 't -> 't)
//                  and 't : (static member ( * ) : 't * 't -> 't)
//                  and 't : (static member ( / ) : 't * 't -> 't)> = 't

// type AbelianGroup<'t when 't : equality
//                         and 't : comparison
//                         and 't : (static member Identity : Unit -> 't)
//                         and 't : (static member Add : 't * 't -> 't)
//                         and 't : (static member Negate : 't -> 't)> = 't                 
// Core type definitions
type IAbelianGroup<'T> =
    abstract member Add: 'T -> 'T -> 'T
    abstract member Neg: 'T -> 'T 
    abstract member Identity: unit -> 'T

type IStream<'T> = 
    abstract member Send : 'T -> unit
    abstract member CurrentTime : int
    abstract member Group : IAbelianGroup<'T>
    abstract member SetDefault : 'T -> unit
    abstract member Item : int -> 'T


type Stream<'T when 'T : comparison> =
    {   mutable Timestamp: int
        Elements: Dictionary<int, 'T>
        GroupOp: IAbelianGroup<'T>
        mutable IsIdentity: bool
        mutable Default: 'T
        DefaultChanges: Dictionary<int, 'T> }

    with
    interface IStream<'T> with
        member this.Send (element: 'T) =
            if element <> this.Default then
                this.Elements.Add(this.Timestamp + 1, element)
                this.IsIdentity <- false
                this.Timestamp <- this.Timestamp + 1
            else
                this.Timestamp <- this.Timestamp + 1

        member this.CurrentTime = this.Timestamp
        member this.Group = this.GroupOp
        member this.SetDefault (newDefault: 'T) = this.Default <- newDefault
        member this.Item (time: int) =
            if time > this.Timestamp then
                this.Default
            else
                match this.Elements.TryGetValue(time) with
                | true, v -> v
                | _ -> this.Default

module Stream =
    let create (groupOp: IAbelianGroup<'T>) =
        let identity = groupOp.Identity()
        {   Timestamp = -1
            Elements = new Dictionary<int, 'T>()
            GroupOp = groupOp
            IsIdentity = true
            Default = identity
            DefaultChanges = Dictionary([KeyValuePair(0, identity)]) }
        |> fun stream -> 
            // Send initial identity element
            { stream with Timestamp = 0 }

    let toSeq (stream : IStream<'t>) =
        seq { 
            for t in 0..stream.CurrentTime do
                yield stream.Item t 
        }

type StreamHandle<'t when 't : comparison> = IStream<'t> ref
 
type IOperator<'t when 't : comparison> = 
    abstract member Step : unit -> bool
    abstract member Output : StreamHandle<'t>

type IUnaryOperator<'tin, 'tout when 'tin : comparison and 'tout : comparison> =
    inherit IOperator<'tout>
    abstract member Input : StreamHandle<'tin>
    

type IBinaryOperator<'A, 'B, 'tout when 'A : comparison and 'B : comparison and 'tout : comparison> =
    inherit IOperator<'tout>
    abstract member Input : StreamHandle<'A>
    abstract member Input2 : StreamHandle<'B>

module Operators =  
    
    type Operator< 'op, 't when 't : comparison and 'op : (static member Step : 'op -> bool) and 'op : ( member Output : StreamHandle<'t>)> =  'op

    type UnaryOperator< 'op, 't when 't : comparison 
        and 'op : (static member Step : 'op -> bool) 
        and 'op : ( member Output : StreamHandle<'t>)
        and 'op : (member Input1 : StreamHandle<'t>)> =  'op

    type BinaryOperator< 'op, 'A, 'B, 'T when 'A : comparison and 'B : comparison and 'T : comparison
        and 'op : (static member Step : 'op -> bool) 
        and 'op : ( member Output : StreamHandle<'A>)
        and 'op : (member Input1 : StreamHandle<'B>)
        and 'op : (member Input2 : StreamHandle<'T>)> =  'op 

    let inline StepUntilFixpoint (op: Operator<'op,'t>) =
        while not <| ('op.Step) (op) do ()
        
    let inline StepUntilFixpointAndReturn (op: Operator<'op,'t>) =
        while not <| ('op.Step) (op) do ()
        (op.Output)

    let inline step_until_fixpoint_set_new_default_then_return (op : UnaryOperator<'op,'t>)  =
        while not <| ('op.Step) (op) do ()
        op.Output.Value.SetDefault (op.Output.Value.Item op.Output.Value.CurrentTime)
        (op.Output)

    type Delay<'t when 't : comparison> =
        { Input1 : StreamHandle<'t>
          Output : StreamHandle<'t> }
        static member Step (op : Delay<'t>) = 
            let output_timestamp = op.Output.Value.CurrentTime
            let input_timestamp = op.Input1.Value.CurrentTime
    
            if output_timestamp <= input_timestamp then
                op.Output.Value.Send (op.Input1.Value.Item output_timestamp)
                false
            else
                true
    let inline delay<'t when 't : comparison> input output = { Input1 = input; Output = output } : UnaryOperator<_,_>

    type Lift1<'t when 't : comparison> =    
        { mutable Frontier : int
          Fn : 't -> 't
          Input1 : StreamHandle<'t>
          Output : StreamHandle<'t> } 
        static member Step (op : Lift1<'t>) =
            let output_timestamp = op.Output.Value.CurrentTime
            let input_timestamp = op.Input1.Value.CurrentTime
            let join = max (max input_timestamp output_timestamp) op.Frontier
            let meet = min (min input_timestamp output_timestamp) op.Frontier
    
            if join = meet then true 
            else
                let next_frontier = op.Frontier + 1
                op.Output.Value.Send (op.Fn (op.Input1.Value.Item next_frontier))
                op.Frontier <- next_frontier
                false

    type Lift2<'A, 'B, 'tout when 'A : comparison and 'B : comparison and 'tout : comparison> =
        { mutable Frontier1 : int
          mutable Frontier2 : int
          Fn : 'A -> 'B -> 'tout
          Input1 : StreamHandle<'A>
          Input2 : StreamHandle<'B>
          Output : StreamHandle<'tout> }
        static member Step (op : Lift2<'A, 'B, 'tout>) =
            let output_timestamp = op.Output.Value.CurrentTime
            let input1_timestamp = op.Input1.Value.CurrentTime
            let input2_timestamp = op.Input2.Value.CurrentTime
            let join = Array.max [|input1_timestamp; input2_timestamp; output_timestamp; op.Frontier1; op.Frontier2|]
            let meet = Array.min [|input1_timestamp; input2_timestamp; output_timestamp; op.Frontier1; op.Frontier2|]
            if join = meet then true 
            else 
        
                let next_frontier_1 = op.Frontier1 + 1
                let next_frontier_2 = op.Frontier2 + 1
                let a = op.Input1.Value.Item next_frontier_1
                let b = op.Input2.Value.Item next_frontier_2
        
                let application = op.Fn a b
                op.Output.Value.Send(application)
        
                op.Frontier1 <- next_frontier_1
                op.Frontier2 <- next_frontier_2
        
                false

    let inline liftBinary (fn: 'A -> 'B -> 'T) (input1: StreamHandle<'A>) (input2: StreamHandle<'B>) (output: StreamHandle<'T>) = 
        {   Frontier1 = -1
            Frontier2 = -1
            Fn = fn
            Input1 = input1
            Input2 = input2
            Output = output }
        : BinaryOperator<_,_,_,_>            
    let inline liftGroupAdd<'t when 't : comparison> (g : IAbelianGroup<'t>) (input1 : StreamHandle<'t>) (input2 : StreamHandle<'t>)(output : StreamHandle<'t>) = 
        {   Frontier1 = -1
            Frontier2 = -1
            Fn = g.Add
            Input1 = input1
            Input2 = input2
            Output = output }
        : BinaryOperator<_,_,_,_>

    let inline liftGroupNeg<'t when 't : comparison> (g : IAbelianGroup<'t>) (input1 : StreamHandle<'t>) (output : StreamHandle<'t>) =
        {   Frontier = -1
            Fn = g.Neg
            Input1 = input1
            Output = output }
        : UnaryOperator<_,_>            

    type Diff<'t when 't : comparison > =
        {
            Input1 : StreamHandle<'t>
            Output : StreamHandle<'t>
            diff : BinaryOperator<Lift2<'t,'t,'t>,'t,'t,'t>
            delayed_neg : UnaryOperator<Lift1<'t>,'t>
            delayed : Delay<'t>
        }       
        static member Step (op : Diff<'t>) =
            Delay.Step op.delayed
            Lift1.Step op.delayed_neg
            Lift2.Step op.diff
            op.diff.Output.Value.CurrentTime = op.Input1.Value.CurrentTime

    let inline diff<'t when 't : comparison> (input : StreamHandle<'t>) (output : StreamHandle<'t>) : UnaryOperator<_,_> =
            let delay = ref (Stream.create input.Value.Group) : StreamHandle<'t>
            let delay_neg = ref (Stream.create input.Value.Group) : StreamHandle<'t>
            let diffOp = { 
                    Input1 = input
                    Output = output
                    diff = liftGroupAdd input.Value.Group input delay_neg output 
                    delayed_neg = liftGroupNeg input.Value.Group delay  delay_neg 
                    delayed = { Input1 = input; Output = delay } }
            diffOp

    type Integrate<'t when 't : comparison > =
        {
            Input1 : StreamHandle<'t>
            Output : StreamHandle<'t>
            intg : BinaryOperator<Lift2<'t,'t,'t>,'t,'t,'t>
            delayed : UnaryOperator<Delay<'t>,'t>
        }            
        static member Step (op : Integrate<'t>) =
            Delay.Step op.delayed
            Lift2.Step op.intg
            op.intg.Output.Value.CurrentTime = op.Input1.Value.CurrentTime

    let inline integrate<'t when 't : comparison> (input : StreamHandle<'t>) (output : StreamHandle<'t>) : UnaryOperator<_,_> = 
        let delay_out = ref (Stream.create input.Value.Group) : StreamHandle<_>
        {
            Input1 = input
            Output = output
            intg = liftGroupAdd input.Value.Group input delay_out output 
            delayed = delay output delay_out 
        } : Integrate<'t>
        


    type Incrementalize<'A, 'B, 'tout when 'A : comparison and 'B : comparison and 'tout : comparison> =
        {
            Input1 : StreamHandle<'A>
            Input2 : StreamHandle<'B>
            Output : StreamHandle<'tout>
            intg_1 : UnaryOperator<Integrate<'A>,'A> 
            delay_intg1 : UnaryOperator<Delay<'A>,'A> 
            intg_2 : UnaryOperator<Integrate<'B>,'B> 
            delay_intg2 : UnaryOperator<Delay<'B>,'B> 
            fn : BinaryOperator<Lift2<'A, 'B, 'tout>,'tout,'A,'B>     
            fn_delay1 : BinaryOperator<Lift2<'A, 'B, 'tout>,'tout,'A,'B>    
            fn_delay2 : BinaryOperator<Lift2<'A, 'B, 'tout>,'tout,'A,'B> 
        }

        //Computes a[t] ⨝ b[t] + z^1(I(a))[t] ⨝ b[t] + a[t] ⨝ z^1(I(b))[t]
        static member Step (op : Incrementalize<'A, 'B, 'tout>) = 
            Integrate.Step op.intg_1
            Delay.Step op.delay_intg1
            Integrate.Step op.intg_2
            Delay.Step op.delay_intg2
            Lift2.Step op.fn
            Lift2.Step op.fn_delay2
            Lift2.Step op.fn_delay1
            

            let ab = op.fn.Output.Value.Item op.fn.Output.Value.CurrentTime
            let adib = op.fn_delay2.Output.Value.Item op.fn_delay2.Output.Value.CurrentTime
            let diab = op.fn_delay1.Output.Value.Item op.fn_delay1.Output.Value.CurrentTime
    
            let group : IAbelianGroup<'tout> = op.Output.Value.Group
            do op.Output.Value.Send(group.Add (group.Add ab adib) diab)
            true
    
    let incrementalize<'A,'B,'T when 'A : comparison and 'B : comparison and 'T : comparison> (fn : 'A -> 'B -> 'T) (input1 : StreamHandle<'A>) (input2 : StreamHandle<'B>) (output: StreamHandle<'T>) : Incrementalize<'A,'B,'T> = 
            let groupOp1 : IAbelianGroup<'A> = input1.Value.Group
            let groupOp2 : IAbelianGroup<'B> = input2.Value.Group
            let integrateHandleA = ref (Stream.create groupOp1) : StreamHandle<'A>
            let integrateHandleB = ref (Stream.create groupOp2) : StreamHandle<'B>
            let delayIntegrateHandleA = ref (Stream.create groupOp1) : StreamHandle<'A>
            let delayIntegrateHandleB = ref (Stream.create groupOp2) : StreamHandle<'B>
            let output1 = ref (Stream.create output.Value.Group) : StreamHandle<'T>
            let output2 = ref (Stream.create output.Value.Group) : StreamHandle<'T>
            let output3 = ref (Stream.create output.Value.Group) : StreamHandle<'T>
            let intg1 : UnaryOperator<Integrate<'A>,'A> = integrate<'A> input1 integrateHandleA
            let delayIntg1 = delay<'A> integrateHandleA delayIntegrateHandleA
            let intg2: UnaryOperator<Integrate<'B>,'B>  = integrate<'A> input2 integrateHandleB
            let delayIntg2 = delay<'B> integrateHandleB delayIntegrateHandleB
            let fn = liftBinary fn input1 input2 output 
            let adib = liftBinary fn input1 delayIntegrateHandleB output1
            let diab = liftBinary fn delayIntegrateHandleA input2 output2
            
            {
                Input1 = input1
                Input2 = input2
                Output = output
                intg_1 = intg1
                delay_intg1 = delayIntg1
                intg_2 = intg2
                delay_intg2 = delayIntg2
                fn = fn
                fn_delay1 = adib
                fn_delay2 = diab
            }





type StreamGroup<'t when 't : comparison>(group : IAbelianGroup<'t>) = 
    interface IAbelianGroup<Stream<'t>> with
        member this.Add (x: Stream<'t>) (y: Stream<'t>) = 
            let identity = group.Identity
            let default1 = x.Default
            let defaultChanges = x.DefaultChanges
            let default2 = y.Default
            let defaultChanges2 = y.DefaultChanges
            let newDefault = group.Add default1 default2
            let newDefaultChanges = 
                defaultChanges
                |> Map.toSeq
                |> Seq.fold (fun acc (k,v) -> 
                    let newV = 
                        match Map.tryFind k defaultChanges2 with
                        | Some v2 -> group.Add v v2
                        | None -> group.Add v default2
                    Map.add k newV acc
                ) Map.empty
                |> fun m -> 
                    defaultChanges2
                    |> Map.toSeq
                    |> Seq.fold (fun acc (k,v) -> 
                        if Map.containsKey k acc then acc
                        else Map.add k (group.Add default1 v) acc
                    ) m
            {   Timestamp = max x.Timestamp y.Timestamp
                Elements = 
                    x.Elements
                    |> Map.toSeq
                    |> Seq.fold (fun acc (k,v) -> 
                        let newV = 
                            match Map.tryFind k y.Elements with
                            | Some v2 -> group.Add v v2
                            | None -> group.Add v default2
                        Map.add k newV acc
                    ) Map.empty
                GroupOp = group
                IsIdentity = x.IsIdentity && y.IsIdentity
                Default = newDefault
                DefaultChanges = newDefaultChanges }      
        member this.Neg (x: Stream<'t>) =
            {   Timestamp = x.Timestamp
                Elements = x.Elements
                |> Map.toSeq
                |> Seq.map (fun (k,v) -> (k, x.GroupOp.Neg v))
                |> Map.ofSeq
                GroupOp = x.GroupOp
                IsIdentity = x.IsIdentity
                Default = x.GroupOp.Neg x.Default
                DefaultChanges = x.DefaultChanges
                |> Map.toSeq
                |> Seq.map (fun (k,v) -> (k, x.GroupOp.Neg v))
                |> Map.ofSeq }          

        member this.Identity() = Stream.create group         

type StreamBuilder<'T when 'T : comparison> () =
    member        _.ReturnFrom (expr) = expr                                        : '``monad<'t>``
    member inline _.Return (x: 'T) = Stream.send x                                        : Stream<'T>
    member inline _.Yield  (x: 'T) = Stream.send x                                       : Stream<'T>
    member inline _.Bind (p: Stream<'T>, [<InlineIfLambda>]rest: 'T->'``Monad<'U>``) = p >>= rest : '``Monad<'U>``
    member inline _.MergeSources  (t1: Stream<'T>, t2: '``Monad<'U>``)          : '``Monad<'T * 'U>`` = Lift2.Invoke tuple2 t1 t2
    member inline _.MergeSources3 (t1: Stream<'T>, t2: '``Monad<'U>``, t3: '``Monad<'V>``) : '``Monad<'T * 'U * 'V>`` = Lift3.Invoke tuple3 t1 t2 t3

    [<CustomOperation("select", MaintainsVariableSpaceUsingBind=true, AllowIntoPattern=true)>]
    member inline _.Select (x, [<ProjectionParameter>] f) = map f x

    [<CustomOperation("where", MaintainsVariableSpaceUsingBind=true)>]
    member inline _.Where (x, [<ProjectionParameter>] p) = mfilter p x

    [<CustomOperation("top", MaintainsVariableSpaceUsingBind=true)>]
    member inline _.Top (source, n) = limit n source

    [<CustomOperation("groupBy", AllowIntoPattern=true, MaintainsVariableSpaceUsingBind=true)>]
    member inline _.GroupBy (x,[<ProjectionParameter>] f : 'T -> 'key) = groupBy f x

    [<CustomOperation("chunkBy", AllowIntoPattern=true, MaintainsVariableSpaceUsingBind=true)>]
    member inline _.ChunkBy (x,[<ProjectionParameter>] f : 'T -> 'key) = chunkBy f x

    [<CustomOperation("orderBy", MaintainsVariableSpaceUsingBind=true, AllowIntoPattern=true)>]
    member inline _.OrderBy (x,[<ProjectionParameter>] f : 'T -> 'key) = sortBy f x


    member inline _.Delay ([<InlineIfLambda>]expr: _->'``Monad<'T>``) = Delay.Invoke expr : '``Monad<'T>``
    member        _.Run f = f                                           : '``monad<'t>``
    member inline _.TryWith    (expr, [<InlineIfLambda>]handler     ) = TryWith.Invoke    expr handler      : '``Monad<'T>``
    member inline _.TryFinally (expr, [<InlineIfLambda>]compensation) = TryFinally.Invoke expr compensation : '``Monad<'T>``
    member inline _.Using (disposable: #IDisposable, [<InlineIfLambda>]body) = Using.Invoke disposable body : '``Monad<'T>``
    