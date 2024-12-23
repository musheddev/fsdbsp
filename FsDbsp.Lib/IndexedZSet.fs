namespace FsDbsp.Lib.IndexedZSet
open System.Collections.Generic

type Index<'K, 'V when 'K : comparison and 'V : comparison> = 'V -> 'K


type IndexedZSet<'T when 'T : comparison> =
    { Values: Dictionary<'T, int>
      Indexes: Dictionary<string, Index<'Field, 'T>> 
      IndexedValues: Dictionary<string, Dictionary<'Field, Dictionary<'T, int>>> }
    
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

    let addIndex name (index: 'K -> 'T) (zset: IndexedZSet<'T>) =
        let indexedVals = new Dictionary<obj, Dictionary<'T,int>>()
        let grouped = zset.Values 
                     |> Seq.groupBy (fun (KeyValue(v,_)) -> index v :> obj)
        
        for (key, group) in grouped do
            let innerDict = new Dictionary<'T,int>()
            for KeyValue(k,v) in group do
                innerDict.Add(k,v)
            indexedVals.Add(key, innerDict)
            
        { zset with 
            Indexes = 
                let newIndexes = cloneDict zset.Indexes
                newIndexes.Add(name, index >> box)
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