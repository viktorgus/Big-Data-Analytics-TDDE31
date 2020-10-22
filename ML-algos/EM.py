from pyspark import SparkContext
import numpy as np

sc = SparkContext(appName = "EM")

data = sc.textFile("data")

# data as cords of points
data = data.map(lambda line: line.split(";"))
data.cache()

# number of dimensions
D = data.columns.size
# clusters
k = 3

# Initiate mean, covariance matrix (sigmas) and phis = p(i) = 1/k
mus = [0] * k 
sigmas = [0] * k
phis = [1/k] * k



for i in range(k):
    randPoint = data.takeSample(False, 1, seed=0)
    # initiate mean i to random point
    mus[i] = np.array(randPoint.collect())
    # start with covariance = identity matrix
    sigmas[i] = np.identity(D)

# mu dim: k * D
mus = sc.broadcast(mus)
#sigma dim : k * D * D
sigmas = sc.broadcast(sigmas)
# phi dim: k
phis = sc.broadcast(phis)




def likelihood(x,mu,sigma,D):
    return ( 1/(2*pow(phi,D/2)*pow(np.linalg.det(sigma),1/2))*exp(-1/2 * np.transpose(x-mu) * np.inverse(sigma) * (x-mu)  )  )

# p(znk | xn, phi, mu, sigma) = phik* likelihood(x|muk, sigmak)/sum(phiP*likelihood(x|muP,sigmaP) for P in 1:k)
def probPointIsClass(x, mus, sigmas, phis, i, k, D):
    return ( phis[k] * likelihood(x,mus[i], sigmas[i], D) /sum( phi[p]*likelihood(x,mus[p],sigmas[p],D) for p in range(k) ) )
    
delta = 100
# limit when to stop algorithm based on sum of change in mean, covMatrix and phis
limit = 0.001
N = data.count()

while(delta > limit):
    # calc p(znk | xn, phi, mu, sigma), x * p(znk | xn, phi, mu, sigma) to calc phi and mu first
    # one at x[2] to sum to N
    delta = 0
    # produces temp results for all k variables (RDD has k*2+1 columns), value one at last column to get total row number N
    temp = data.map(lambda x: (  probPointIsClass(x,mus,sigmas,phis,i, k, D), x * probPointIsClass(x,mus,sigmas,phis,i, k, D)   for i in range(0,k-1))  )
    params = temp.reduce(lambda x1,x2: x1+x2)
    # for all k, get mu = sum(x*p(zi|ui,sigmai))/sum(p(zi|ui,sigmai))
    muML = (params[i*2+1]/params[i*2] for i in range(0,k-1))
    # for all k, get phi = sum(p(zi|ui,sigmai))/N)
    phiML = (params[i*2]/N for i in range(0,k-1))

    #update delta i.e change in parameters
    delta += abs(muML-mus) + abs(phiML-phis)

    mus = sc.broadcast(muML)
    phis = sc.broadcast(phiML)

    # to calculate the uptadted covariance matrix get denominator and enumerator for the sums
    temp = data.map(lambda x: (   (x-mus[i])*np.transpose(x - mus[i]) * probPointIsClass(x,mus,sigmas,phis,i, k, D), probPointIsClass(x,mus,sigmas,phis,i, k, D)   for i in range(k))  )
    sigmaParams = temp.reduce(lambda x,y: x+y)
    sigmaML = (sigmaParams[2*i]/sigmaParams[2*i+1] for i in range(k))

    delta += abs(sigmaML-sigmas)

    sigmas = sc.broadcast(sigmaML)
