namespace FsDbsp.Lib.IndexedZSet
open System.Collections.Generic
open System



type IndexedZSet<'T when 'T : comparison> =
    { Values: Dictionary<'T, int>
      Indexes: Dictionary<string, obj>
      IndexedValues: Dictionary<string, Dictionary<IComparable, Dictionary<'T, int>>> }
    
    static member Empty = 
        { Values = new Dictionary<_,_>()
          Indexes = new Dictionary<_,_>()
          IndexedValues = new Dictionary<_,_>() }

module IndexedZSet =
    let private cloneDict (dict: Dictionary<'K,'V>) =
        let newDict = new Dictionary<'K,'V>()
        for KeyValue(k,v) in dict do
            newDict.Add(k,v)
        newDict

    let addIndex<'T when 'T : comparison> name (index: 'K -> 'T) (zset: IndexedZSet<'T>) =
        let indexedVals = new Dictionary<IComparable, Dictionary<'T,int>>()
        let grouped = zset.Values 
                     |> Seq.groupBy (fun (KeyValue(v,_)) -> index v )
        
        for (key, group) in grouped do
            let innerDict = new Dictionary<'T,int>()
            for KeyValue(k,v) in group do
                innerDict.Add(k,v)
            indexedVals.Add(key, innerDict)
            
        { zset with 
            Indexes = 
                let newIndexes = cloneDict zset.Indexes
                newIndexes.Add(name, (box index ) )
                newIndexes
            IndexedValues = 
                let newVals = cloneDict zset.IndexedValues
                newVals.Add(name, indexedVals)
                newVals }

    let select (predicate: 'T -> bool) (zset: IndexedZSet<'T>) =
        let newVals = new Dictionary<'T,int>()
        for KeyValue(k,v) in zset.Values do
            if predicate k then
                newVals.Add(k,v)
                
        { zset with Values = newVals }

    let project (projection: 'T -> 'R) (zset: IndexedZSet<'T>) =
        let projected = new Dictionary<'R,int>()
        for KeyValue(k,v) in zset.Values do
            let projectedKey = projection k
            match projected.TryGetValue(projectedKey) with
            | true, existing -> projected.[projectedKey] <- existing + v
            | false, _ -> projected.Add(projectedKey, v)
            
        IndexedZSet<'R>.Empty
        |> fun z -> { z with Values = projected }

    let H (diff: IndexedZSet<'T>) (integrated: IndexedZSet<'T>) =
        let result = new Dictionary<'T,int>()
        for KeyValue(k,v) in diff.Values do
            match integrated.Values.TryGetValue(k) with
            | true, curr ->
                let total = v + curr
                if curr > 0 && total <= 0 then 
                    result.Add(k,-1)
                elif curr <= 0 && total > 0 then
                    result.Add(k,1)
            | false, _ -> 
                if v > 0 then result.Add(k,1)
                
        { IndexedZSet.Empty with Values = result }




module V2 = 
    type AppendOnlySpine<'T when 'T : comparison>() =
        let mutable len = 0
        let load = 1024
        let lists = List<List<'T>>()
        let maxes = List<'T>()
        
        member _.Length = len
        
        member _.Add(value: 'T) =
            if maxes.Count > 0 then
                let pos = 
                    let rec binarySearch left right =
                        if left >= right then right
                        else
                            let mid = (left + right) / 2
                            if maxes.[mid] <= value then 
                                binarySearch (mid + 1) right
                            else 
                                binarySearch left mid
                    binarySearch 0 maxes.Count
                    
                let targetPos = 
                    if pos = maxes.Count then pos - 1
                    else pos
                    
                let targetList = lists.[targetPos]
                
                // Insert sorted
                let insertPos = 
                    targetList |> Seq.tryFindIndex (fun x -> x > value)
                    |> Option.defaultValue targetList.Count
                targetList.Insert(insertPos, value)
                
                if pos = maxes.Count then
                    maxes.[targetPos] <- value
                    
                // Expand if needed
                if targetList.Count > load then
                    let splitAt = targetList.Count / 2
                    let newList = targetList.GetRange(splitAt, targetList.Count - splitAt)
                    targetList.RemoveRange(splitAt, targetList.Count - splitAt)
                    lists.Insert(targetPos + 1, newList)
                    maxes.Insert(targetPos + 1, newList.[newList.Count - 1])
            else
                lists.Add(List<'T>([value]))
                maxes.Add(value)

        member _.Expand(pos: int) =
            if lists.[pos].Count > (load <<< 1) then
                let splitList = lists.[pos]
                let half = splitList.GetRange(load, splitList.Count - load)
                splitList.RemoveRange(load, splitList.Count - load)
                maxes.[pos] <- splitList.[splitList.Count - 1]
                
                lists.Insert(pos + 1, half)
                maxes.Insert(pos + 1, half.[half.Count - 1])

        member _.GetEnumerator() =
            seq {
                for sublist in lists do
                    yield! sublist
            }

// Sort merge join implementation
    let sortMergeJoin (spine1: AppendOnlySpine<'T>) (spine2: AppendOnlySpine<'T>) =
        seq {
            use enum1 = spine1.GetEnumerator()
            use enum2 = spine2.GetEnumerator()
            
            let mutable hasMore1 = enum1.MoveNext()
            let mutable hasMore2 = enum2.MoveNext()
            
            while hasMore1 && hasMore2 do
                let item1 = enum1.Current
                let item2 = enum2.Current
                
                if item1 < item2 then
                    hasMore1 <- enum1.MoveNext()
                elif item2 < item1 then  
                    hasMore2 <- enum2.MoveNext()
                else
                    yield item1
                    hasMore1 <- enum1.MoveNext()
                    hasMore2 <- enum2.MoveNext()
        }

    type Indexer<'T,'I> = 'T -> 'I

    type IndexedZSet<'I,'T when 'I : comparison and 'T : comparison> = 
        { Inner: Dictionary<'T,int>
          IndexToValue: Dictionary<'I,HashSet<'T>>
          Indexer: 'T -> 'I
          Index: AppendOnlySpine<'I> }
    
    module IndexedZSet =
        let create (values: Dictionary<'T,int>) (indexer: 'T -> 'I) =
            let zset = 
                { Inner = new Dictionary<'T,int>()
                  IndexToValue = new Dictionary<'I,HashSet<'T>>()
                  Indexer = indexer 
                  Index = AppendOnlySpine<'I>() }
            
            // Initialize with values
            for KeyValue(k,v) in values do
                let indexedValue = indexer k
                match zset.IndexToValue.TryGetValue(indexedValue) with
                | true, set -> 
                    set.Add(k) |> ignore
                | false, _ ->
                    let set = HashSet<'T>()
                    set.Add(k) |> ignore
                    zset.IndexToValue.Add(indexedValue, set)
                zset.Inner.Add(k,v)
                zset.Index.Add(indexedValue)
            zset
            
        let contains key zset =
            zset.Inner.ContainsKey(key)
            
        let tryGet key zset =
            match zset.Inner.TryGetValue(key) with
            | true, v -> Some v
            | false, _ -> None
            
        let get key zset =
            match tryGet key zset with
            | Some v -> v
            | None -> 0
            
        let set key value zset =
            zset.Inner.[key] <- value
            let indexedValue = zset.Indexer key
            match zset.IndexToValue.TryGetValue(indexedValue) with
            | true, set ->
                set.Add(key) |> ignore
            | false, _ ->
                let set = HashSet<'T>()
                set.Add(key) |> ignore
                zset.IndexToValue.Add(indexedValue, set)
            zset.Index.Add(indexedValue)
            zset
    
    type IndexedZSetAddition<'I,'T when 'I : comparison and 'T : comparison>
        (innerGroup: ZSetAddition<'T>, indexer: 'T -> 'I) =
        
        member _.Add(a: IndexedZSet<'I,'T>, b: IndexedZSet<'I,'T>) =
            let newInner = Dictionary<'T,int>()
            for KeyValue(k,v) in a.Inner do
                newInner.Add(k,v)
            for KeyValue(k,v) in b.Inner do
                match newInner.TryGetValue(k) with
                | true, existing -> newInner.[k] <- existing + v
                | false, _ -> newInner.Add(k,v)
            IndexedZSet.create newInner indexer
            
        member _.Identity() =
            IndexedZSet.create (Dictionary<'T,int>()) indexer
            
        member _.Equal(a: IndexedZSet<'I,'T>, b: IndexedZSet<'I,'T>) =
            if a.Inner.Count <> b.Inner.Count then false
            else
                let mutable equal = true
                use enum1 = a.Inner.GetEnumerator()
                while equal && enum1.MoveNext() do
                    match b.Inner.TryGetValue(enum1.Current.Key) with
                    | true, v when v = enum1.Current.Value -> ()
                    | _ -> equal <- false
                equal
    
        interface IAbelianGroup<IndexedZSet<'I,'T>> with
            member this.Add(a, b) = this.Add(a, b)
            member this.Identity() = this.Identity()
            member this.Equal(a, b) = this.Equal(a, b)