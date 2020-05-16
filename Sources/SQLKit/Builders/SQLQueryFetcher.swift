import NIO

/// A `SQLQueryBuilder` that supports decoding results.
///
///     builder.all(decoding: Planet.self)
///
public protocol SQLQueryFetcher: SQLQueryBuilder { }

extension SQLQueryFetcher {
    // MARK: First

    public func first<D>(decoding: D.Type) -> EventLoopFuture<D?>
        where D: Decodable
    {
        self.first().flatMapThrowing {
            guard let row = $0 else {
                return nil
            }
            return try row.decode(model: D.self)
        }
    }
    
    /// Collects the first raw output and returns it.
    ///
    ///     builder.first()
    ///
    public func first() -> EventLoopFuture<SQLRow?> {
        return self.all().map { $0.first }
    }
    
    /// Collects all decoded output into an array and returns it.
    ///
    ///     builder.all(decoding: Planet.self)
    ///
    public func all<D>(decoding: D.Type) -> EventLoopFuture<[D]>
        where D: Decodable
    {
        self.all().flatMapThrowing {
            try $0.map {
                try $0.decode(model: D.self)
            }
        }
    }
    
    /// Decodes two types from the result set. Collects all decoded output into an array and returns it.
    ///
    ///     builder.all(decoding: Planet.self, Galaxy.self)
    ///
    public func all<A, B>(decoding: A.Type, _ b: B.Type) -> EventLoopFuture<[(A, B)]>
        where A: Decodable, B: Decodable
    {
        self.all().flatMapThrowing {
            try $0.map {
                let a = try $0.decode(model: A.self)
                let b = try $0.decode(model: B.self)
                return (a, b)
            }
        }
    }
    
    /// Decodes three types from the result set. Collects all decoded output into an array and returns it.
    ///
    ///     builder.all(decoding: Planet.self, Galaxy.self, SolarSystem.self)
    ///
    public func all<A, B, C>(decoding: A.Type, _ b: B.Type, _ c: C.Type) -> EventLoopFuture<[(A, B, C)]>
        where A: Decodable, B: Decodable, C: Decodable
    {
        self.all().flatMapThrowing {
            try $0.map {
                let a = try $0.decode(model: A.self)
                let b = try $0.decode(model: B.self)
                let c = try $0.decode(model: C.self)
                return (a, b, c)
            }
        }
    }
    
    /// Collects all raw output into an array and returns it.
    ///
    ///     builder.all()
    ///
    public func all() -> EventLoopFuture<[SQLRow]> {
        var all: [SQLRow] = []
        return self.run { row in
            all.append(row)
        }.map { all }
    }
    
    // MARK: Run


    public func run<D>(decoding: D.Type, _ handler: @escaping (Result<D, Error>) -> ()) -> EventLoopFuture<Void>
        where D: Decodable
    {
        self.run {
            do {
                try handler(.success($0.decode(model: D.self)))
            } catch {
                handler(.failure(error))
            }
        }
    }
    
    
    /// Runs the query, passing output to the supplied closure as it is recieved.
    ///
    ///     builder.run { print($0) }
    ///
    /// The returned future will signal completion of the query.
    public func run(_ handler: @escaping (SQLRow) -> ()) -> EventLoopFuture<Void> {
        return self.database.execute(sql: self.query) { row in
            handler(row)
        }
    }
}
