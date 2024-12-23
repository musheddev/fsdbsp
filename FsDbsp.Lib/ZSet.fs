namespace FsDbsp.Lib.ZSet
open System.Collections.Generic
        
type ZSet<'T when 'T : comparison> =  Dictionary<'T, int>
    


module ZSet =
    // Linear Operations
    let select (predicate: 'T -> bool) (zset: ZSet<'T>) =
        zset
        |> Seq.filter (fun kv -> predicate kv.Key)
        |> ZSet
        
    let project (projection: 'T -> 'R) (zset: ZSet<'T>) =
        zset
        |> Seq.fold (fun acc (kv) ->
            let projected = projection kv.Key
            match Map.tryFind projected acc with
            | Some existing -> Map.add projected (existing + v) acc
            | None -> Map.add projected v acc
        ) Dictionary<_,_>()
        |> ZSet
        
    // Binary Operations
    let H (diff: ZSet<'T>) (integratedState: ZSet<'T>) =
        diff
        |> Seq.choose (fun (kv) ->
          let mutable wasFound = Unchecked.defaultof<_>
          let valueRef = &CollectionsMarshal.GetValueRefOrAddDefault (dictionary, key, &wasFound)
        
            match Map.tryFind k integratedState.Items with
            | Some currentState ->
                let coalescedWeight = v + currentState
                if currentState > 0 && coalescedWeight <= 0 then
                    Some (k, -1)
                elif currentState <= 0 && coalescedWeight > 0 then
                    Some (k, 1)
                else None
            | None ->
                if v > 0 then Some (k, 1) else None)
        |> ZSet
        
    // Bilinear Operations
    let join 
        (leftZSet: ZSet<'T>) 
        (rightZSet: ZSet<'R>) 
        (joinPredicate: 'T -> 'R -> bool)
        (projection: 'T -> 'R -> 'S) =
        
        leftZSet.Items
        |> Map.toSeq
        |> Seq.collect (fun (leftValue, leftWeight) ->
            rightZSet.Items
            |> Map.toSeq
            |> Seq.choose (fun (rightValue, rightWeight) ->
                if joinPredicate leftValue rightValue then
                    let projected = projection leftValue rightValue
                    Some (projected, leftWeight * rightWeight)
                else None))
        |> Seq.groupBy fst
        |> Seq.map (fun (k,v) -> k, v |> Seq.sumBy snd)
        |> ZSet.Create        