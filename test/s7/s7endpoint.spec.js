//@ts-check
/*jshint esversion: 6, node: true*/

const { expect } = require('chai');
const net = require('net');
const { Duplex } = require('stream');
const S7Endpoint = require('../../src/s7endpoint');

const sleep = ms => new Promise(res => setTimeout(res, ms));

/**
 * A fake PLC that accepts TCP connections but never answers the COTP
 * connection request, keeping track of the sockets still open on its side
 */
function createSilentServer() {
    const open = new Set();
    const server = net.createServer(sock => {
        open.add(sock);
        sock.on('data', () => { });
        sock.on('close', () => open.delete(sock));
        sock.on('error', () => { });
    });
    return new Promise(res => server.listen(0, '127.0.0.1', () => res({
        port: /** @type {net.AddressInfo} */ (server.address()).port,
        open,
        close: () => {
            for (const s of open) s.destroy();
            return new Promise(r => server.close(r));
        }
    })));
}

class StubTransport extends Duplex {
    constructor() {
        super();
        this.destroyedByLib = false;
    }
    _read() { }
    _write(chunk, enc, cb) { cb(); }
    destroy(err) {
        this.destroyedByLib = true;
        return super.destroy(err);
    }
}

describe('S7Endpoint connection attempts', () => {

    it('should destroy the pending transport when the connection times out', async () => {
        const plc = await createSilentServer();
        const ep = new S7Endpoint({ host: '127.0.0.1', port: plc.port, autoReconnect: 60000, connectTimeout: 200 });
        try {
            const err = await new Promise(res => ep.once('error', res));
            expect(err.code).to.equal('ERR_TIMEOUT');
            await sleep(100);
            expect(plc.open.size).to.equal(0);
        } finally {
            await ep.disconnect();
            await plc.close();
        }
    });

    it('should not leak a socket on every reconnection attempt', async () => {
        const plc = await createSilentServer();
        const ep = new S7Endpoint({ host: '127.0.0.1', port: plc.port, autoReconnect: 50, connectTimeout: 150 });
        let errors = 0;
        ep.on('error', () => errors++);
        try {
            while (errors < 3) await sleep(20);
            await sleep(50);
            // at most the socket of an attempt that is in flight right now
            expect(plc.open.size).to.be.at.most(1);
        } finally {
            await ep.disconnect();
            await sleep(50);
            expect(plc.open.size).to.equal(0);
            await plc.close();
        }
    });

    it('should destroy and not adopt a transport that arrives after the timeout', async () => {
        const late = new StubTransport();
        let resolveLate;
        const ep = new S7Endpoint({
            autoReconnect: 60000,
            connectTimeout: 100,
            customTransport: () => new Promise(res => { resolveLate = res; })
        });
        try {
            const err = await new Promise(res => ep.once('error', res));
            expect(err.code).to.equal('ERR_TIMEOUT');
            resolveLate(late);
            await sleep(20);
            expect(late.destroyedByLib).to.equal(true);
            expect(ep._transport).to.not.equal(late);
        } finally {
            await ep.disconnect();
        }
    });

    it('should resolve disconnect() while a connection attempt is pending', async () => {
        const plc = await createSilentServer();
        const ep = new S7Endpoint({ host: '127.0.0.1', port: plc.port, autoReconnect: 60000, connectTimeout: 5000 });
        const errors = [];
        ep.on('error', e => errors.push(e));
        try {
            while (plc.open.size < 1) await sleep(10);
            await ep.disconnect();
            await sleep(50);
            expect(plc.open.size).to.equal(0);
            expect(errors).to.have.length(0);
        } finally {
            await plc.close();
        }
    });

    it('should not start a new attempt after disconnect()', async () => {
        let calls = 0;
        const ep = new S7Endpoint({
            autoReconnect: 100,
            connectTimeout: 50,
            customTransport: () => { calls++; return new Promise(() => { }); }
        });
        ep.on('error', () => { });
        await new Promise(res => ep.once('error', res)); // first attempt timed out, reconnect scheduled
        expect(calls).to.equal(1);
        await ep.disconnect();
        await sleep(300);
        expect(calls).to.equal(1);
    });

    it('should still connect and disconnect normally', async () => {
        const t = new StubTransport();
        const ep = new S7Endpoint({ autoReconnect: 0, customTransport: async () => t });
        ep.on('error', () => { });
        ep.connect().catch(() => { });
        await sleep(20);
        expect(ep._transport).to.equal(t);
        await ep.disconnect();
        expect(t.destroyedByLib).to.equal(true);
        expect(ep._transport).to.equal(null);
    });
});
