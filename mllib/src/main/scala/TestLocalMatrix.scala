import org.apache.spark.mllib.linalg.{Matrices, Matrix, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint

/**
  * Created by wzb on 2017/8/31.
  */
object TestLocalMatrix extends App{

  val a = Array(1.0, 0.0, 3.0)
  val s1 = Vectors.dense(a)
  val s2 = Vectors.sparse(3, Array(0, 2), Array(1.0, 3.0))

  val pos = LabeledPoint(1.0, Vectors.dense(1.0, 0.0, 3.0))

  // Create a dense matrix ((1.0, 2.0), (3,0,4.0), (5.0,6.0)
  val dm: Matrix = Matrices.dense(3, 2, Array(1.0, 3.0, 5.0, 2.0, 4.0, 6.0))

  // Create a sparse matrix((9.0, 0.0), (0.0, 8.0),  (6.0, 0.0))
  /**
    * 稀疏矩阵
    * Column-major sparse matrix.
    * The entry values are stored in Compressed Sparse Column (CSC) format.
    * For example, the following matrix
    * {{{
    *   1.0 0.0 4.0
    *   0.0 3.0 5.0
    *   2.0 0.0 6.0
    * }}}
    * is stored as `values: [1.0, 2.0, 3.0, 4.0, 5.0, 6.0]`,
    * `rowIndices=[0, 2, 1, 0, 1, 2]`, `colPointers=[0, 2, 3, 6]`.
    * 这里设置了三个参数是为了即能表示元素在矩阵中的位置又能表示在值数组中的位置
    * 以colPointers(列指针)中2为例，值为2，数组下标为1，所以是第二列，2是值数组的下标，对应值为3.0,同时对应row（行）的下标为2的值是1
    * 所以矩阵中位置(1,1)的元素值为3.0
    *
    */
  val sm: Matrix = Matrices.sparse(3, 2, Array(0, 1, 3), Array(0, 1, 2), Array(9, 8, 6))

  println(dm)
  println(sm)


}
