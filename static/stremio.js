var stremioUser;

async function stremioDirectAuth() {
    const authKey = document.getElementById("stremio-authkey").value.trim();
    
    if (!authKey) {
        console.log("Please enter an auth key");
        return;
    }

    try {
        // Test the auth key by trying to load addons
        const addonCollection = await stremioAddonCollectionGet(authKey);
        
        if (addonCollection.error) {
            console.log("Invalid auth key");
            return;
        }

        // If successful, create a minimal user object similar to login response
        stremioUser = {
            result: {
                authKey: authKey
            }
        };

        // Update UI
        document.getElementById("desc-head").style.display = "none";
        document.querySelector(".login-group").style.display = "none";
        document.querySelector(".add-group").style.display = "flex";
        document.querySelector(".translate-button").style.display = "flex";
        
        // Load addons
        await stremioLoadAddons(authKey);
    } catch (error) {
        console.log("Error authenticating with auth key:", error);
    }
}

async function stremioLogin() {
    const email = document.getElementById("stremio-email").value;
    const password = document.getElementById("stremio-password").value;
    const facebook = document.getElementById("facebook-toggle").checked;
    const loginUrl = "https://api.strem.io/api/login";
    const loginData = {
        "type": "Login",
        "email": email,
        "password": password,
        "facebook": facebook
    }
    const response = await fetch(loginUrl, {
        method: "POST",
        headers: {
            "Content-Type": "application/json"
        },
        body: JSON.stringify(loginData)
    })
    .then(response => response.json());
    
    if (response.error) {
        console.log(response.error.message);
    } else {
        stremioUser = response;
        document.getElementById("desc-head").style.display = "none";
        document.querySelector(".login-group").style.display = "none";
        document.querySelector(".add-group").style.display = "flex";
        document.querySelector(".translate-button").style.display = "flex";
        await stremioLoadAddons(response.result.authKey);
    }
}

async function stremioLoadAddons(authKey) {
    const addonCollection = await stremioAddonCollectionGet(authKey);
    // Load Addons
    for(var i=0; i<addonCollection.result.addons.length; i++) {
        var url = addonCollection.result.addons[i].transportUrl;
        await loadAddon(url);
    }
}

async function stremioAddonCollectionGet(authKey) {
    const addonCollectionUrl = "https://api.strem.io/api/addonCollectionGet"

    const payload = {
        "type": "AddonCollectionGet",
        "authKey": authKey,
        "update": true
    }

    const response = await fetch(addonCollectionUrl, {
        method: "POST",
        headers: {
            "Content-Type": "application/json"
        },
        body: JSON.stringify(payload)
    });

    return await response.json();
}

async function stremioAddonCollectionSet(authKey, addonList) {
    const addonCollectionUrl = "https://api.strem.io/api/addonCollectionSet"

    const payload = {
        "type": "AddonCollectionSet",
        "authKey": authKey,
        "addons": addonList
    }

    const response = await fetch(addonCollectionUrl, {
        method: "POST",
        headers: {
            "Content-Type": "application/json"
        },
        body: JSON.stringify(payload)
    });

    return await response.json();
}