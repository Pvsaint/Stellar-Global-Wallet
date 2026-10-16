import * as StellarSdk from "@stellar/stellar-sdk";
import { config } from "../config";
import { SorobanSimulationError, SorobanTransactionError } from "./sorobanErrors";
import { getCachedSimulate, setCachedSimulate } from "./sorobanCache";

const { rpc } = StellarSdk;

// Soroban RPC's getTransaction reports NOT_FOUND until the ledger closes
// (~5s on testnet/mainnet). Polling every 1.5s up to 30s comfortably covers
// normal ledger close time plus retries without hanging a request forever.
const POLL_INTERVAL_MS = 1_500;
const POLL_TIMEOUT_MS = 30_000;

export interface ContractCallResult<T = unknown> {
  result: T;
  hash: string;
  ledger: number;
}

export class SorobanService {
  private readonly server: StellarSdk.rpc.Server;
  private readonly networkPassphrase: string;

  constructor() {
    this.networkPassphrase = config.SOROBAN_NETWORK_PASSPHRASE;
    this.server = new rpc.Server(config.SOROBAN_RPC_URL);
  }

  /**
   * Simulates a contract call and returns the decoded return value without
   * ever submitting a transaction. Use for read-only functions (e.g.
   * `get_assets`) — free, no state change, no signing key required. The
   * given `sourcePublicKey` only pays the (simulated, never-charged)
   * transaction's nominal source-account role; it does not need to match
   * any address referenced by the call.
   */
  async simulate<T = unknown>(params: {
    contractId: string;
    method: string;
    args: StellarSdk.xdr.ScVal[];
    sourcePublicKey: string;
  }): Promise<T> {
    const { contractId, method, args, sourcePublicKey } = params;

    // #86 — Redis cache: encode args as base64 XDR for a stable cache key.
    // scValToNative is not round-trippable, so we key on the raw XDR bytes.
    const argsKey = Buffer.from(args.map((a) => a.toXDR("base64")).join("|")).toString(
      "base64"
    );
    const cached = await getCachedSimulate<T>(contractId, method, argsKey);
    if (cached !== null) return cached;

    const sourceAccount = await this.server.getAccount(sourcePublicKey);
    const tx = this.buildInvocation(sourceAccount, contractId, method, args);

    const sim = await this.server.simulateTransaction(tx);
    if (rpc.Api.isSimulationError(sim)) {
      throw new SorobanSimulationError(
        `Simulation failed for ${contractId}.${method}: ${sim.error}`,
        sim.error
      );
    }
    const result = this.parseSimResult<T>(sim);
    await setCachedSimulate(contractId, method, argsKey, result);
    return result;
  }

  /**
   * Simulates, then — only if simulation succeeds — assembles, signs, and
   * submits the invocation, polling until the network reports a final
   * status. `sourceSecretKey` both pays the fee and provides the signature;
   * for globe-wallet's user-scoped functions (e.g. `record_spend`) this must
   * be the same account passed as the `user` argument, since that's whose
   * `require_auth()` the contract checks.
   */
  async invoke<T = unknown>(params: {
    contractId: string;
    method: string;
    args: StellarSdk.xdr.ScVal[];
    sourceSecretKey: string;
  }): Promise<ContractCallResult<T>> {
    const { contractId, method, args, sourceSecretKey } = params;
    const keypair = StellarSdk.Keypair.fromSecret(sourceSecretKey);
    const sourceAccount = await this.server.getAccount(keypair.publicKey());
    const tx = this.buildInvocation(sourceAccount, contractId, method, args);

    // Simulation-before-submission: this is what lets us reject a doomed
    // call (bad args, insufficient auth, a contract error like
    // SpendLimitExceeded) before ever paying network fees or waiting on
    // ledger close. sendTransaction is never reached below this point on a
    // failed simulation.
    const sim = await this.server.simulateTransaction(tx);
    if (rpc.Api.isSimulationError(sim)) {
      throw new SorobanSimulationError(
        `Simulation failed for ${contractId}.${method}: ${sim.error}`,
        sim.error
      );
    }

    const prepared = rpc.assembleTransaction(tx, sim).build();
    prepared.sign(keypair);

    const send = await this.server.sendTransaction(prepared);
    if (send.status === "ERROR") {
      throw new SorobanSimulationError(
        `${contractId}.${method} was rejected before entering the ledger (status: ${send.status})`,
        send.errorResult ? send.errorResult.toXDR("base64") : send.status
      );
    }

    const final = await this.pollTransaction(send.hash);
    if (final.status === rpc.Api.GetTransactionStatus.SUCCESS) {
      const result =
        final.returnValue !== undefined
          ? (StellarSdk.scValToNative(final.returnValue) as T)
          : (undefined as T);
      return { result, hash: send.hash, ledger: final.ledger };
    }

    if (final.status === rpc.Api.GetTransactionStatus.FAILED) {
      throw new SorobanTransactionError(
        `${contractId}.${method} passed simulation but failed on-chain (hash ${send.hash})`,
        send.hash
      );
    }

    throw new SorobanTransactionError(
      `${contractId}.${method} did not reach a final status within ${POLL_TIMEOUT_MS}ms (hash ${send.hash})`,
      send.hash
    );
  }

  private buildInvocation(
    sourceAccount: StellarSdk.Account,
    contractId: string,
    method: string,
    args: StellarSdk.xdr.ScVal[]
  ) {
    const contract = new StellarSdk.Contract(contractId);
    return new StellarSdk.TransactionBuilder(sourceAccount, {
      fee: StellarSdk.BASE_FEE,
      networkPassphrase: this.networkPassphrase,
    })
      .addOperation(contract.call(method, ...args))
      .setTimeout(30)
      .build();
  }

  private parseSimResult<T>(sim: StellarSdk.rpc.Api.SimulateTransactionSuccessResponse): T {
    if (!sim.result) return undefined as T;
    return StellarSdk.scValToNative(sim.result.retval) as T;
  }

  private async pollTransaction(
    hash: string
  ): Promise<StellarSdk.rpc.Api.GetTransactionResponse> {
    const deadline = Date.now() + POLL_TIMEOUT_MS;
    let response = await this.server.getTransaction(hash);
    while (
      response.status === rpc.Api.GetTransactionStatus.NOT_FOUND &&
      Date.now() < deadline
    ) {
      await new Promise((resolve) => setTimeout(resolve, POLL_INTERVAL_MS));
      response = await this.server.getTransaction(hash);
    }
    return response;
  }
}
