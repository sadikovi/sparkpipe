package org.sparkpipe.mllib.linalg

import breeze.linalg.{DenseMatrix => BDM, DenseVector => BDV}
import com.github.fommil.netlib.ARPACK
import org.netlib.util.{doubleW, intW}

/** Compute eigen values and vectors. Derived from Spark MLLib */
private[mllib] object EigenValueDecomposition {
    def symmetricEigs(
        m: BDM[Double],
        n: Int,
        k: Int,
        tol: Double,
        maxIterations: Int): (BDV[Double], BDM[Double]
    ) = {
        require(n > k, s"Number of required eigenvalues $k must be smaller than matrix dimension $n")

        val arpack = ARPACK.getInstance()

        // tolerance used in stopping criterion
        val tolW = new doubleW(tol)
        // number of desired eigenvalues, 0 < nev < n
        val nev = new intW(k)
        // nev Lanczos vectors are generated in the first iteration
        // ncv-nev Lanczos vectors are generated in each subsequent iteration
        // ncv must be smaller than n
        val ncv = math.min(2 * k, n)

        // "I" for standard eigenvalue problem, "G" for generalized eigenvalue problem
        val bmat = "I"
        // "LM" : compute the NEV largest (in magnitude) eigenvalues
        val which = "LM"

        var iparam = new Array[Int](11)
        // use exact shift in each iteration
        iparam(0) = 1
        // maximum number of Arnoldi update iterations, or the actual number of iterations on output
        iparam(2) = maxIterations
        // Mode 1: A*x = lambda*x, A symmetric
        iparam(6) = 1

        require(n * ncv.toLong <= Integer.MAX_VALUE && ncv * (ncv.toLong + 8) <= Integer.MAX_VALUE,
            s"k = $k and/or n = $n are too large to compute an eigendecomposition")

        var ido = new intW(0)
        var info = new intW(0)
        var resid = new Array[Double](n)
        var v = new Array[Double](n * ncv)
        var workd = new Array[Double](n * 3)
        var workl = new Array[Double](ncv * (ncv + 8))
        var ipntr = new Array[Int](11)

        // call ARPACK's reverse communication, first iteration with ido = 0
        arpack.dsaupd(ido, bmat, n, which, nev.`val`, tolW, resid, ncv, v, n, iparam, ipntr, workd,
            workl, workl.length, info)

        val w = BDV(workd)

        // ido = 99 : done flag in reverse communication
        while (ido.`val` != 99) {
            if (ido.`val` != -1 && ido.`val` != 1) {
                throw new IllegalStateException("ARPACK returns ido = " + ido.`val` +
                    " This flag is not compatible with Mode 1: A*x = lambda*x, A symmetric.")
            }
            // multiply working vector with the matrix
            val inputOffset = ipntr(0) - 1
            val outputOffset = ipntr(1) - 1
            val x = w.slice(inputOffset, inputOffset + n)
            val y = w.slice(outputOffset, outputOffset + n)
            y := m * x
            // call ARPACK's reverse communication
            arpack.dsaupd(ido, bmat, n, which, nev.`val`, tolW, resid, ncv, v, n, iparam, ipntr,
                workd, workl, workl.length, info)
        }

        if (info.`val` != 0) {
            info.`val` match {
                case 1 =>
                    throw new IllegalStateException("ARPACK returns non-zero info = " + info.`val` +
                        " Maximum number of iterations taken. Refer ARPACK user guide for details")
            case 3 =>
                throw new IllegalStateException("ARPACK returns non-zero info = " + info.`val` +
                    " No shifts could be applied. Try to increase NCV. " +
                    "(Refer ARPACK user guide for details)")
            case _ =>
                throw new IllegalStateException("ARPACK returns non-zero info = " + info.`val` +
                    " Please refer ARPACK user guide for error message.")
            }
        }

        val d = new Array[Double](nev.`val`)
        val select = new Array[Boolean](ncv)
        // copy the Ritz vectors
        val z = java.util.Arrays.copyOfRange(v, 0, nev.`val` * n)

        // call ARPACK's post-processing for eigenvectors
        arpack.dseupd(true, "A", select, d, z, n, 0.0, bmat, n, which, nev, tol, resid, ncv, v, n,
            iparam, ipntr, workd, workl, workl.length, info)

        // number of computed eigenvalues, might be smaller than k
        val computed = iparam(4)

        val eigenPairs = java.util.Arrays.copyOfRange(d, 0, computed).zipWithIndex.map { r =>
            (r._1, java.util.Arrays.copyOfRange(z, r._2 * n, r._2 * n + n))
        }

        // sort the eigen-pairs in descending order
        val sortedEigenPairs = eigenPairs.sortBy(- _._1)

        // copy eigenvectors in descending order of eigenvalues
        val sortedU = BDM.zeros[Double](n, computed)
        sortedEigenPairs.zipWithIndex.foreach { r =>
            val b = r._2 * n
            var i = 0
            while (i < n) {
                sortedU.data(b + i) = r._1._2(i)
                i += 1
            }
        }

        (BDV[Double](sortedEigenPairs.map(_._1)), sortedU)
    }
}
