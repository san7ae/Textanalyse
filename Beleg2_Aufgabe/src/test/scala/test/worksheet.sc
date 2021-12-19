import scala.util.Random

def createHashFuntions(prim:Integer, nrHashFuns: Int):Array[(Int=>Int)]= {
  val m = if(prim > nrHashFuns) Random.nextInt(prim) else throw new Exception("Number of Hashfunction must be greater than prime number")
  val b = Random.nextInt(prim)
  (for (i <- 0 until nrHashFuns) yield (x : Int) => ((m*x) + b)%prim).toArray
}

val arr = createHashFuntions(127, 23)

println(arr(2)(5))