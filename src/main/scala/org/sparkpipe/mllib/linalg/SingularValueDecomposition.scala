package org.sparkpipe.mllib.linalg

import java.util.Arrays
import breeze.linalg.{DenseMatrix => BDM, DenseVector => BDV}
import breeze.numerics.{sqrt => brzSqrt}

/** Compute singular values. Derived from Spark MLLib */
object SingularValueDecomposition {
    /** Solve SVD and return result in general form */
    private def generalSVD(
        a: BDM[Double],
        k: Int,
        rCond: Double,
        tolerance: Double
    ): Tuple4[Int, Int, Array[Double], Array[Double]] = {
        val numRows = a.rows
        val numCols = a.cols
        // Compute Gramian matrix, eigen values give us "S" and "U" in this case
        val m = a * a.t
        // Number of iterations for computing Eigen vectors
        val maxIterations: Int = 300
        val eigRes = EigenValueDecomposition.symmetricEigs(m, numRows, k, tolerance, maxIterations)
        val (squaredSigmas, u) = eigRes
        val sigmas = brzSqrt(squaredSigmas)
        // Determine the effective rank
        val sigma0 = sigmas(0)
        val threshold = rCond * sigma0
        var i = 0
        // "sigmas" might have a length smaller than k, if some Ritz values do not satisfy the
        // convergence criterion specified by tol after max number of iterations.
        // Thus use i < min(k, sigmas.length) instead of i < k
        if (sigmas.length < k) {
            println(s"Requested $k singular values but only found ${sigmas.length} converged.")
        }

        while (i < math.min(k, sigmas.length) && sigmas(i) >= threshold) {
            i += 1
        }

        val sk = i

        if (sk < k) {
            println(s"Requested $k singular values but only found $sk nonzeros.")
        }

        // results
        val sData = Arrays.copyOfRange(sigmas.data, 0, sk)
        val uData = Arrays.copyOfRange(u.data, 0, numRows * sk)
        (numRows, sk, sData, uData)
    }

    /**
     * SVD solver for Breeze matrices.
     * @param a matrix A to compute SVD
     * @param k top singular values to keep
     * @param computeV whether or not to compute V-factor, V will be null, if false
     * @param rCond reciprocal condition number. All singular values smaller than rCond * sigma(0)
     *              are treated as zero, where sigma(0) is the largest singular value.
     * @param tolerance tolerance for computing eigen values, default is 1E-7
     * @return Tuple [U, s, V]
     */
    def solveSVD(
        a: BDM[Double],
        k: Int,
        computeV: Boolean,
        rCond: Double = 1E-7,
        tolerance: Double = 1E-7
    ): Tuple3[BDM[Double], BDV[Double], BDM[Double]] = {
        val (numRows, sk, sData, uData) = generalSVD(a, k, rCond, tolerance)
        val s = BDV(sData)
        val U = new BDM(numRows, sk, uData)
        val V = if (computeV) {
            val N = new BDM[Double](numRows, sk, Arrays.copyOfRange(uData, 0, uData.length))
            var i = 0
            var j = 0
            while (j < sk) {
                i = 0
                val sigma = sData(j)
                while (i < numRows) {
                    N(i, j) /= sigma
                    i += 1
                }
                j += 1
            }
            N.t * a
        } else {
            null
        }
        (U, s, V)
    }
}
