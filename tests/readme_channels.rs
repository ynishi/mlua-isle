#![cfg(feature = "tokio")]
//! The "Channels and select (async)" example of the README.

use mlua_isle::runtime::{channel, channel_to_host, Config, Vm};
use mlua_isle::AsyncIsle;

#[tokio::test]
async fn readme_channels_and_select() -> Result<(), Box<dyn std::error::Error>> {
    let (isle, driver, (events, mut reports)) = AsyncIsle::builder()
        .spawn_with(|lua| {
            let vm = Vm::attach(lua, Config::default())?;
            lua.globals().set("task", vm.task_lib()?)?;
            let (events, inbox) = channel::<String>(lua, 16)?;
            let (outbox, reports) = channel_to_host::<String>(lua, 16)?;
            lua.globals().set("inbox", inbox)?;
            lua.globals().set("outbox", outbox)?;
            Ok((events, reports))
        })
        .await?;

    let main = isle.spawn_coroutine_eval::<()>(
        r#"
        while true do
          local stop = task.select({
            inbox:on(function(ev, ok)
              if not ok then return true end   -- the host dropped its Sender
              outbox:send("got " .. ev)
              return false
            end),
            task.after(1000):on(function()
              outbox:send("idle")
              return false
            end),
          })
          if stop then return end
        end
        "#,
    );
    events.send("a".to_string()).await?;
    assert_eq!(reports.recv().await.as_deref(), Some("got a"));
    drop(events); // closes `inbox`: the loop returns
    main.await?;
    driver.shutdown().await?;
    Ok(())
}
