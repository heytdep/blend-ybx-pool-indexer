use serde::{Deserialize, Serialize};
use zephyr_sdk::{
    prelude::*,
    soroban_sdk::{xdr::ScVal, Symbol},
    utils::address_to_alloc_string,
    DatabaseDerive, EnvClient, PrettyContractEvent,
};

#[derive(Serialize, Deserialize, Clone, Copy)]
#[repr(u32)]
pub enum Action {
    Borrow,
    Collateral,
}

#[derive(DatabaseDerive, Serialize)]
#[with_name("actions")]
pub struct Actions {
    pub action: u32,
    pub timestamp: u64,
    pub ledger: u32,
    pub asset: String,
    pub source: String,
    pub amount: i64,
}

impl Actions {
    fn new(
        env: &EnvClient,
        action: Action,
        timestamp: u64,
        ledger: u32,
        asset: ScVal,
        amount: i128,
        source: ScVal,
    ) -> Self {
        let asset = address_to_alloc_string(env, env.from_scval(&asset));
        let source = address_to_alloc_string(env, env.from_scval(&source));
        Self {
            action: action as u32,
            timestamp,
            ledger,
            asset,
            amount: amount as i64,
            source,
        }
    }

    fn add(env: &EnvClient, action: Action, event: PrettyContractEvent, increase: bool) {
        let (amount, _): (i128, i128) = env.from_scval(&event.data);
        let delta = if increase { amount } else { -amount };
        let supply = Actions::new(
            env,
            action,
            env.reader().ledger_timestamp(),
            env.reader().ledger_sequence(),
            event.topics[1].clone(),
            delta,
            event.topics[2].clone(),
        );
        env.put(&supply);
    }
}

const CONTRACT: &'static str = "CBP7NO6F7FRDHSOFQBT2L2UWYIZ2PU76JKVRYAQTG3KZSQLYAOKIF2WB";

#[no_mangle]
pub extern "C" fn on_close() {
    let env = EnvClient::new();
    let ybx_contract = stellar_strkey::Contract::from_string(&CONTRACT).unwrap().0;
    let searched_events: Vec<PrettyContractEvent> = {
        let events = env.reader().pretty().soroban_events();
        events
            .iter()
            .filter_map(|x| {
                if x.contract == ybx_contract {
                    Some(x.clone())
                } else {
                    None
                }
            })
            .collect()
    };

    for event in searched_events {
        let action: Symbol = env.from_scval(&event.topics[0]);
        if action == Symbol::new(env.soroban(), "supply_collateral") {
            Actions::add(&env, Action::Collateral, event, true);
        } else if action == Symbol::new(env.soroban(), "withdraw_collateral") {
            Actions::add(&env, Action::Collateral, event, false);
        } else if action == Symbol::new(env.soroban(), "borrow") {
            Actions::add(&env, Action::Borrow, event, true);
        } else if action == Symbol::new(env.soroban(), "repay") {
            Actions::add(&env, Action::Borrow, event, false);
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct Request {
    kind: Action,
    address: Option<String>,
    // Add additional filters here
}

#[no_mangle]
pub extern "C" fn retrieve() {
    let env = EnvClient::empty();
    let request: Request = env.read_request_body();

    let actions: Vec<Actions> = if let Some(address) = request.address {
        env.read_filter()
            .column_equal_to("action", request.kind as u32)
            .column_equal_to("source", address)
            .read()
            .unwrap()
    } else {
        env.read_filter()
            .column_equal_to("action", request.kind as u32)
            .read()
            .unwrap()
    };

    env.conclude(&actions)
}

#[cfg(test)]
mod test {
    use ledger_meta_factory::TransitionPretty;
    use stellar_xdr::next::{Hash, Int128Parts, Limits, ScSymbol, ScVal, ScVec, WriteXdr};
    use zephyr_sdk::testutils::TestHost;

    #[test]
    fn print() {
        println!(
            "{}",
            ScVal::Symbol(ScSymbol("supply_collateral".try_into().unwrap()))
                .to_xdr_base64(Limits::none())
                .unwrap()
        );
        println!(
            "{}",
            ScVal::Symbol(ScSymbol("withdraw_collateral".try_into().unwrap()))
                .to_xdr_base64(Limits::none())
                .unwrap()
        );

        println!(
            "{}",
            ScVal::Symbol(ScSymbol("borrow".try_into().unwrap()))
                .to_xdr_base64(Limits::none())
                .unwrap()
        );
        println!(
            "{}",
            ScVal::Symbol(ScSymbol("repay".try_into().unwrap()))
                .to_xdr_base64(Limits::none())
                .unwrap()
        );
    }

    fn add_deposit(transition: &mut TransitionPretty) {
        transition.inner.set_sequence(2000);
        transition
            .contract_event(
                "CBP7NO6F7FRDHSOFQBT2L2UWYIZ2PU76JKVRYAQTG3KZSQLYAOKIF2WB",
                vec![
                    ScVal::Symbol(ScSymbol("supply_collateral".try_into().unwrap())),
                    ScVal::Address(stellar_xdr::next::ScAddress::Contract(Hash([8; 32]))),
                    ScVal::Address(stellar_xdr::next::ScAddress::Contract(Hash([1; 32]))),
                ],
                ScVal::Vec(Some(ScVec(
                    vec![
                        ScVal::I128(Int128Parts {
                            hi: 0,
                            lo: 1000000000,
                        }),
                        ScVal::I128(Int128Parts {
                            hi: 0,
                            lo: 500000000,
                        }),
                    ]
                    .try_into()
                    .unwrap(),
                ))),
            )
            .unwrap();
    }

    fn add_withdraw(transition: &mut TransitionPretty) {
        transition.inner.set_sequence(2010);
        transition
            .contract_event(
                "CBP7NO6F7FRDHSOFQBT2L2UWYIZ2PU76JKVRYAQTG3KZSQLYAOKIF2WB",
                vec![
                    ScVal::Symbol(ScSymbol("withdraw_collateral".try_into().unwrap())),
                    ScVal::Address(stellar_xdr::next::ScAddress::Contract(Hash([8; 32]))),
                    ScVal::Address(stellar_xdr::next::ScAddress::Contract(Hash([1; 32]))),
                ],
                ScVal::Vec(Some(ScVec(
                    vec![
                        ScVal::I128(Int128Parts {
                            hi: 0,
                            lo: 1000000000,
                        }),
                        ScVal::I128(Int128Parts {
                            hi: 0,
                            lo: 500000000,
                        }),
                    ]
                    .try_into()
                    .unwrap(),
                ))),
            )
            .unwrap();
    }

    #[tokio::test]
    async fn withdraw() {
        let env = TestHost::default();
        let mut program = env.new_program("./target/wasm32-unknown-unknown/release/blend_ybx.wasm");

        let mut db = env.database("postgres://postgres:postgres@localhost:5432");
        db.load_table(
            0,
            "actions",
            vec!["action", "timestamp", "ledger", "asset", "source", "amount"],
        )
        .await;

        assert_eq!(db.get_rows_number(0, "actions").await.unwrap(), 0);

        let mut empty = TransitionPretty::new();
        program.set_transition(empty.inner.clone());

        let invocation = program.invoke_vm("on_close").await;
        assert!(invocation.is_ok());
        let inner_invocation = invocation.unwrap();
        assert!(inner_invocation.is_ok());

        assert_eq!(db.get_rows_number(0, "actions").await.unwrap(), 0);

        // After deposit

        add_deposit(&mut empty);
        program.set_transition(empty.inner.clone());

        let invocation = program.invoke_vm("on_close").await;
        assert!(invocation.is_ok());
        let inner_invocation = invocation.unwrap();
        assert!(inner_invocation.is_ok());

        assert_eq!(db.get_rows_number(0, "actions").await.unwrap(), 1);

        // After deposit + withdrawal

        add_withdraw(&mut empty);
        program.set_transition(empty.inner);

        let invocation = program.invoke_vm("on_close").await;
        assert!(invocation.is_ok());
        let inner_invocation = invocation.unwrap();
        assert!(inner_invocation.is_ok());

        assert_eq!(db.get_rows_number(0, "actions").await.unwrap(), 3);

        db.close().await
    }
}
