"use strict";

const shared = require("@connected-car/shared");

describe("remote-commands baseline", () => {
  test("@connected-car/shared exports schemas and listSchemas", () => {
    expect(shared).toBeTruthy();
    expect(shared.schemas).toBeTruthy();
    expect(typeof shared.listSchemas).toBe("function");

    const keys = shared.listSchemas();
    expect(Array.isArray(keys)).toBe(true);
    expect(keys).toEqual(expect.arrayContaining(["remoteCommand.v1", "commandAck.v1"]));
  });
});
